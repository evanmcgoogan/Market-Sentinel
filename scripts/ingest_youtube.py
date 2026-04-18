"""
ingest_youtube.py — Poll YouTube channels for new videos and fetch transcripts.

Uses RSS feeds for video discovery (free, no API key) and youtube-transcript-api
for transcript fetching. Writes immutable raw files to raw/transcripts/{channel}/.

Usage:
    python scripts/ingest_youtube.py                         # Poll all channels
    python scripts/ingest_youtube.py --channel 20vc          # Poll one channel
    python scripts/ingest_youtube.py --dry-run               # Show what would be fetched

No API key required for basic operation.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
import xml.etree.ElementTree as ET
from typing import Any

import aiohttp

from brain_io import (
    append_log,
    is_duplicate,
    load_sources_config,
    record_hash,
    slugify,
    today_str,
    utcnow,
    write_raw_file,
)

logger = logging.getLogger(__name__)

YOUTUBE_RSS_BASE = "https://www.youtube.com/feeds/videos.xml"
MAX_TRANSCRIPT_RETRIES = 2
# YouTube RSS feeds have known intermittent 404 outages (platform-side issue, Feb-Apr 2026+).
# We retry with backoff to handle transient failures gracefully.
RSS_RETRY_ATTEMPTS = 3
RSS_RETRY_BACKOFF_S = 5.0


# ---------------------------------------------------------------------------
# RSS feed parsing
# ---------------------------------------------------------------------------

async def fetch_channel_feed(
    session: aiohttp.ClientSession, channel_id: str
) -> list[dict[str, str]]:
    """Fetch recent videos from a YouTube channel's RSS feed.

    Returns list of dicts with: video_id, title, published, link.

    Retries up to RSS_RETRY_ATTEMPTS times with backoff to handle YouTube's
    intermittent RSS 404 outages (platform-side issue, not a channel ID problem).
    """
    url = f"{YOUTUBE_RSS_BASE}?channel_id={channel_id}"
    last_status: int | None = None

    for attempt in range(RSS_RETRY_ATTEMPTS):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return parse_atom_feed(text)
                last_status = resp.status
                if attempt < RSS_RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(RSS_RETRY_BACKOFF_S * (attempt + 1))
        except Exception as e:
            logger.error("RSS fetch error for %s (attempt %d): %s", channel_id, attempt + 1, e)
            if attempt < RSS_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(RSS_RETRY_BACKOFF_S * (attempt + 1))

    logger.warning(
        "RSS fetch failed for %s after %d attempts: HTTP %s",
        channel_id, RSS_RETRY_ATTEMPTS, last_status or "connection error",
    )
    return []


def parse_atom_feed(xml_text: str) -> list[dict[str, str]]:
    """Parse a YouTube Atom feed into a list of video metadata dicts."""
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }

    videos = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.error("XML parse error: %s", e)
        return []

    for entry in root.findall("atom:entry", ns):
        video_id_el = entry.find("yt:videoId", ns)
        title_el = entry.find("atom:title", ns)
        published_el = entry.find("atom:published", ns)
        link_el = entry.find("atom:link", ns)

        if video_id_el is None or title_el is None:
            continue

        videos.append({
            "video_id": video_id_el.text or "",
            "title": title_el.text or "",
            "published": (published_el.text or "")[:10],  # YYYY-MM-DD
            "link": link_el.get("href", "") if link_el is not None else "",
        })

    return videos


# ---------------------------------------------------------------------------
# Transcript fetching
# ---------------------------------------------------------------------------

def fetch_transcript_sync(video_id: str) -> tuple[str, str]:
    """Fetch transcript for a video using youtube-transcript-api.

    Returns (transcript_text, method) where method is 'youtube-transcript-api'
    or 'unavailable'.

    Runs synchronously — called from async context via run_in_executor.
    """
    try:
        from youtube_transcript_api import YouTubeTranscriptApi

        ytt_api = YouTubeTranscriptApi()
        transcript = ytt_api.fetch(video_id)

        # Build readable text from transcript snippets
        lines = []
        for snippet in transcript:
            text = snippet.text if hasattr(snippet, "text") else snippet.get("text", "")
            text = text.strip()
            if text:
                # Clean up auto-caption artifacts
                text = re.sub(r"\[.*?\]", "", text).strip()
                if text:
                    lines.append(text)

        if lines:
            return "\n".join(lines), "youtube-transcript-api"

        logger.warning("Empty transcript for %s", video_id)
        return "", "unavailable"

    except ImportError:
        logger.error("youtube-transcript-api not installed")
        return "", "unavailable"
    except Exception as e:
        logger.warning("Transcript unavailable for %s: %s", video_id, e)
        return "", "unavailable"


async def fetch_transcript(video_id: str) -> tuple[str, str]:
    """Async wrapper around synchronous transcript fetch."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, fetch_transcript_sync, video_id)


# ---------------------------------------------------------------------------
# Transcript formatting
# ---------------------------------------------------------------------------

def estimate_duration(transcript_text: str) -> int:
    """Rough estimate of video duration in minutes from transcript word count.

    Average speaking rate: ~150 words per minute.
    """
    word_count = len(transcript_text.split())
    return max(1, round(word_count / 150))


def format_transcript_body(title: str, transcript: str, video_id: str) -> str:
    """Format transcript as readable markdown body."""
    lines = [
        f"# {title}",
        "",
        f"https://www.youtube.com/watch?v={video_id}",
        "",
        "---",
        "",
        "## Transcript",
        "",
    ]

    # Break transcript into paragraphs (roughly every 5 sentences)
    sentences = re.split(r"(?<=[.!?])\s+", transcript)
    paragraph: list[str] = []
    for i, sentence in enumerate(sentences):
        paragraph.append(sentence)
        if len(paragraph) >= 5:
            lines.append(" ".join(paragraph))
            lines.append("")
            paragraph = []
    if paragraph:
        lines.append(" ".join(paragraph))
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Core ingestion logic
# ---------------------------------------------------------------------------

async def ingest_channel(
    session: aiohttp.ClientSession,
    name: str,
    channel_id: str,
    tier: str,
    domains: list[str],
    dry_run: bool = False,
) -> dict[str, int]:
    """Ingest new videos from a single YouTube channel.

    Returns stats: {videos_found, videos_new, transcripts_fetched, files_written}.
    """
    stats = {"videos_found": 0, "videos_new": 0, "transcripts_fetched": 0, "files_written": 0}

    if not channel_id or channel_id == "PLACEHOLDER":
        logger.warning("Skipping %s — no channel_id configured", name)
        return stats

    videos = await fetch_channel_feed(session, channel_id)
    stats["videos_found"] = len(videos)

    if not videos:
        logger.info("No videos found for %s", name)
        return stats

    channel_slug = slugify(name)

    for video in videos:
        video_id = video["video_id"]

        if is_duplicate("youtube", video_id):
            continue

        stats["videos_new"] += 1

        if dry_run:
            logger.info("[DRY RUN] Would fetch transcript for: %s — %s", video_id, video["title"])
            continue

        # Fetch transcript
        transcript, method = await fetch_transcript(video_id)

        if not transcript:
            logger.info("No transcript available for %s — %s (will retry later)", video_id, video["title"])
            # Don't record hash — we'll try again next poll
            continue

        stats["transcripts_fetched"] += 1

        # Write raw file
        title_slug = slugify(video["title"])
        relative_path = f"raw/transcripts/{channel_slug}/{video_id}--{title_slug}.md"
        duration = estimate_duration(transcript)

        frontmatter = {
            "source": "youtube",
            "channel": name,
            "channel_slug": channel_slug,
            "tier": tier,
            "domains": domains,
            "video_id": video_id,
            "title": video["title"],
            "published_at": video["published"],
            "duration_minutes": duration,
            "collected_at": utcnow(),
            "transcript_method": method,
        }

        body = format_transcript_body(video["title"], transcript, video_id)
        write_raw_file(relative_path, frontmatter, body)
        record_hash("youtube", video_id)
        stats["files_written"] += 1

        # Be polite between transcript fetches
        await asyncio.sleep(0.5)

    if not dry_run and stats["videos_new"] > 0:
        append_log(
            f"INGEST_YOUTUBE {name} | "
            f"found: {stats['videos_found']} | "
            f"new: {stats['videos_new']} | "
            f"transcripts: {stats['transcripts_fetched']} | "
            f"files: {stats['files_written']}"
        )

    return stats


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def ingest_all(
    channel_filter: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run YouTube ingestion for all configured channels (or one if filtered)."""
    config = load_sources_config()
    youtube_config = config.get("youtube", {})
    channels = youtube_config.get("channels") or []

    if not channels:
        logger.warning("No YouTube channels configured in sources.yaml")
        return {"channels_processed": 0}

    if channel_filter:
        channels = [
            c for c in channels
            if c.get("name", "").lower() == channel_filter.lower()
            or slugify(c.get("name", "")) == channel_filter.lower()
        ]
        if not channels:
            logger.error("Channel '%s' not found in sources.yaml", channel_filter)
            return {"error": f"channel_not_found: {channel_filter}"}

    totals = {
        "channels_processed": 0,
        "videos_found": 0,
        "videos_new": 0,
        "transcripts_fetched": 0,
        "files_written": 0,
    }

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
    ) as session:
        for channel in channels:
            name = channel.get("name", "")
            channel_id = channel.get("channel_id", "")
            tier = channel.get("tier", "C")
            domains = channel.get("domains", [])

            if not name:
                continue

            logger.info("Ingesting YouTube channel: %s", name)
            stats = await ingest_channel(session, name, channel_id, tier, domains, dry_run)

            totals["channels_processed"] += 1
            for key in ["videos_found", "videos_new", "transcripts_fetched", "files_written"]:
                totals[key] += stats.get(key, 0)

            await asyncio.sleep(0.5)

    logger.info(
        "YouTube ingestion complete: %d channels, %d new videos, %d transcripts, %d files",
        totals["channels_processed"],
        totals["videos_new"],
        totals["transcripts_fetched"],
        totals["files_written"],
    )

    if not dry_run:
        append_log(
            f"INGEST_YOUTUBE BATCH complete | "
            f"channels: {totals['channels_processed']} | "
            f"new_videos: {totals['videos_new']} | "
            f"transcripts: {totals['transcripts_fetched']} | "
            f"files: {totals['files_written']}"
        )

    return totals


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest transcripts from configured YouTube channels")
    parser.add_argument("--channel", help="Ingest only this channel (by name or slug)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched without writing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    result = asyncio.run(ingest_all(channel_filter=args.channel, dry_run=args.dry_run))
    if "error" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
