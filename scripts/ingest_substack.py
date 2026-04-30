"""
ingest_substack.py — Poll newsletters (Substack, Stratechery, and any RSS feed).

Fetches RSS feeds for configured newsletter publications. Stores each post as a
raw article file with extracted body content. Automatically detects paywalled
posts (content truncated to teaser) and flags them in frontmatter — the
extraction pipeline can still get value from the lede and key framing even
when the full essay isn't available.

No API key required. Works for any RSS feed, not just Substack.

Output: raw/articles/{publication-slug}/YYYY-MM-DD--{slug}.md

Usage:
    python scripts/ingest_substack.py               # Poll all newsletters
    python scripts/ingest_substack.py --name "Not Boring"
    python scripts/ingest_substack.py --dry-run

Paywall detection:
    Posts with < PAYWALL_WORD_THRESHOLD words in body are flagged paywall: true.
    The abstract/lede is still extracted and stored for signal extraction.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from typing import Any

import aiohttp

from brain_io import (
    append_log,
    is_duplicate,
    load_sources_config,
    record_hash,
    slugify,
    utcnow,
    write_raw_file,
)

logger = logging.getLogger(__name__)

PAYWALL_WORD_THRESHOLD = 150   # Posts below this are likely paywalled teasers
MAX_BODY_WORDS = 8_000         # Truncate very long essays for storage efficiency
REQUEST_TIMEOUT_S = 20
REQUEST_DELAY_S = 1.0


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_substack_config() -> dict[str, Any]:
    """Load substack section from sources.yaml."""
    config = load_sources_config()
    return config.get("substack", {})


# ---------------------------------------------------------------------------
# HTML → plain text
# ---------------------------------------------------------------------------

class _HTMLStripper(HTMLParser):
    """Minimal HTML stripper that preserves paragraph breaks."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []
        self._in_head = False

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag in ("head", "style", "script"):
            self._in_head = True
        if tag in ("p", "br", "div", "h1", "h2", "h3", "h4", "li"):
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("head", "style", "script"):
            self._in_head = False

    def handle_data(self, data: str) -> None:
        if not self._in_head:
            self._parts.append(data)

    def get_text(self) -> str:
        raw = "".join(self._parts)
        # Collapse excessive whitespace while preserving paragraph breaks
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        raw = re.sub(r"[ \t]+", " ", raw)
        return raw.strip()


def html_to_text(html: str) -> str:
    """Strip HTML tags, returning clean plain text."""
    if not html:
        return ""
    stripper = _HTMLStripper()
    try:
        stripper.feed(html)
        return stripper.get_text()
    except Exception:
        # Fallback: crude regex strip
        return re.sub(r"<[^>]+>", " ", html).strip()


# ---------------------------------------------------------------------------
# RSS parsing
# ---------------------------------------------------------------------------

def parse_rss_feed(xml_text: str) -> list[dict[str, Any]]:
    """Parse an RSS 2.0 or Atom feed into a list of post dicts.

    Returns dicts with: guid, title, url, published, author, body_html,
    body_text, word_count, paywall.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.error("RSS parse error: %s", e)
        return []

    # Handle both RSS 2.0 and Atom
    ns_atom = "http://www.w3.org/2005/Atom"
    if root.tag == f"{{{ns_atom}}}feed":
        return _parse_atom(root, ns_atom)
    return _parse_rss2(root)


def _parse_rss2(root: ET.Element) -> list[dict[str, Any]]:
    """Parse RSS 2.0 format."""
    posts = []
    ns = {
        "dc": "http://purl.org/dc/elements/1.1/",
        "content": "http://purl.org/rss/1.0/modules/content/",
    }

    for item in root.findall(".//item"):
        title_el = item.find("title")
        link_el = item.find("link")
        guid_el = item.find("guid")
        pub_el = item.find("pubDate")
        # NOTE: use explicit None checks — ElementTree leaf Elements are falsy
        # (no children), so `el or fallback` would always fall through to fallback.
        author_el = item.find("dc:creator", ns)
        if author_el is None:
            author_el = item.find("author")
        # Prefer content:encoded (full text) over description (summary/teaser)
        body_el = item.find("content:encoded", ns)
        if body_el is None:
            body_el = item.find("description")

        title = (title_el.text or "").strip() if title_el is not None else ""
        url = (link_el.text or "").strip() if link_el is not None else ""
        guid = (guid_el.text or url).strip() if guid_el is not None else url
        author = (author_el.text or "").strip() if author_el is not None else ""
        body_html = (body_el.text or "") if body_el is not None else ""
        published = _parse_date(pub_el.text if pub_el is not None else "")

        if not title or not guid:
            continue

        body_text = html_to_text(body_html)
        word_count = len(body_text.split())
        paywall = word_count < PAYWALL_WORD_THRESHOLD and bool(body_html)

        posts.append({
            "guid": guid,
            "title": title,
            "url": url,
            "published": published,
            "author": author,
            "body_html": body_html,
            "body_text": body_text,
            "word_count": word_count,
            "paywall": paywall,
        })

    return posts


def _parse_atom(root: ET.Element, ns_str: str) -> list[dict[str, Any]]:
    """Parse Atom feed format."""
    ns = {"atom": ns_str}
    posts = []

    for entry in root.findall("atom:entry", ns):
        title_el = entry.find("atom:title", ns)
        id_el = entry.find("atom:id", ns)
        link_el = entry.find("atom:link[@rel='alternate']", ns) or entry.find("atom:link", ns)
        pub_el = entry.find("atom:published", ns) or entry.find("atom:updated", ns)
        author_el = entry.find("atom:author/atom:name", ns)
        summary_el = entry.find("atom:summary", ns)
        content_el = entry.find("atom:content", ns)

        title = (title_el.text or "").strip() if title_el is not None else ""
        guid = (id_el.text or "").strip() if id_el is not None else ""
        url = link_el.get("href", "") if link_el is not None else ""
        author = (author_el.text or "").strip() if author_el is not None else ""
        published = _parse_date(pub_el.text if pub_el is not None else "")

        body_html = ""
        if content_el is not None:
            body_html = content_el.text or ""
        elif summary_el is not None:
            body_html = summary_el.text or ""

        if not title or not guid:
            continue

        body_text = html_to_text(body_html)
        word_count = len(body_text.split())
        paywall = word_count < PAYWALL_WORD_THRESHOLD and bool(body_html)

        posts.append({
            "guid": guid,
            "title": title,
            "url": url or guid,
            "published": published,
            "author": author,
            "body_html": body_html,
            "body_text": body_text,
            "word_count": word_count,
            "paywall": paywall,
        })

    return posts


def _parse_date(raw: str) -> str:
    """Extract YYYY-MM-DD from various date formats."""
    if not raw:
        return utcnow().strftime("%Y-%m-%d")

    raw = raw.strip()

    # ISO 8601
    m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)

    # RFC 2822 (e.g. "Thu, 10 Apr 2025 12:00:00 +0000")
    m = re.search(r"(\d{1,2}) (\w{3}) (\d{4})", raw)
    if m:
        months = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
        }
        day, mon_str, year = m.group(1), m.group(2), m.group(3)
        mon = months.get(mon_str, "01")
        return f"{year}-{mon}-{int(day):02d}"

    return utcnow().strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Raw file formatting
# ---------------------------------------------------------------------------

def format_article_body(post: dict[str, Any], publication: str) -> str:
    """Format a newsletter post as readable markdown."""
    lines = [
        f"# {post['title']}",
        "",
        f"**Publication:** {publication}",
    ]
    if post["author"]:
        lines.append(f"**Author:** {post['author']}")
    lines += [
        f"**Published:** {post['published']}",
        f"**URL:** {post['url']}",
    ]

    if post["paywall"]:
        lines += [
            "",
            "> ⚠️ **Paywalled post** — only teaser/intro available. "
            "Subscribe for full content.",
        ]

    lines += [
        "",
        "---",
        "",
        "## Content",
        "",
    ]

    body = post["body_text"].strip()
    if not body:
        body = "_No text content available (paywalled or empty)._"
    elif len(body.split()) > MAX_BODY_WORDS:
        # Truncate very long essays — extract.py doesn't need the full thing
        words = body.split()
        body = " ".join(words[:MAX_BODY_WORDS]) + "\n\n_[Truncated for storage — full content at URL above]_"

    lines.append(body)
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Ingestion logic
# ---------------------------------------------------------------------------

async def fetch_rss(session: aiohttp.ClientSession, rss_url: str) -> list[dict[str, Any]]:
    """Fetch and parse an RSS/Atom feed. Returns list of post dicts."""
    try:
        async with session.get(
            rss_url,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S),
            headers={"User-Agent": "SignalHunter/3.0 (research feed reader)"},
        ) as resp:
            if resp.status != 200:
                logger.warning("RSS fetch failed for %s: HTTP %d", rss_url, resp.status)
                return []
            text = await resp.text()
    except Exception as e:
        logger.error("RSS fetch error for %s: %s", rss_url, e)
        return []

    return parse_rss_feed(text)


async def ingest_newsletter(
    session: aiohttp.ClientSession,
    name: str,
    author: str,
    rss_url: str,
    tier: str,
    domains: list[str],
    dry_run: bool = False,
) -> dict[str, int]:
    """Ingest new posts from a single newsletter RSS feed."""
    stats = {"posts_found": 0, "posts_new": 0, "files_written": 0}

    posts = await fetch_rss(session, rss_url)
    stats["posts_found"] = len(posts)

    if not posts:
        logger.info("No posts found for %s (%s)", name, rss_url)
        return stats

    pub_slug = slugify(name)

    for post in posts:
        guid = post["guid"]
        # Use URL as fallback unique ID if guid is an absolute URL
        unique_id = guid if len(guid) < 200 else post["url"]

        if is_duplicate("substack", unique_id):
            continue

        stats["posts_new"] += 1

        if dry_run:
            paywall_tag = " [PAYWALLED]" if post["paywall"] else ""
            logger.info(
                "[DRY RUN] Would write: %s%s — %s (%d words)",
                post["title"][:60], paywall_tag, post["published"], post["word_count"]
            )
            continue

        date_str = post["published"]
        title_slug = slugify(post["title"])
        relative_path = f"raw/articles/{pub_slug}/{date_str}--{title_slug}.md"

        frontmatter: dict[str, Any] = {
            "source": "substack",
            "publication": name,
            "publication_slug": pub_slug,
            "author": author or post["author"],
            "tier": tier,
            "domains": domains,
            "title": post["title"],
            "published_at": post["published"],
            "url": post["url"],
            "word_count": post["word_count"],
            "paywall": post["paywall"],
            "collected_at": utcnow(),
        }

        body = format_article_body(post, name)
        write_raw_file(relative_path, frontmatter, body)
        record_hash("substack", unique_id)
        stats["files_written"] += 1

    if not dry_run and stats["posts_new"] > 0:
        append_log(
            f"INGEST_SUBSTACK {name} | "
            f"found: {stats['posts_found']} | "
            f"new: {stats['posts_new']} | "
            f"files: {stats['files_written']}"
        )

    return stats


async def ingest_all(
    name_filter: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run newsletter ingestion for all configured publications."""
    config = load_substack_config()
    newsletters = config.get("newsletters", [])

    if not newsletters:
        logger.warning("No newsletters configured in sources.yaml under 'substack:'")
        return {"newsletters_processed": 0}

    if name_filter:
        newsletters = [
            n for n in newsletters
            if n.get("name", "").lower() == name_filter.lower()
            or slugify(n.get("name", "")) == slugify(name_filter)
        ]
        if not newsletters:
            logger.error("Newsletter '%s' not found in sources.yaml", name_filter)
            return {"error": f"newsletter_not_found: {name_filter}"}

    totals: dict[str, Any] = {
        "newsletters_processed": 0,
        "posts_found": 0,
        "posts_new": 0,
        "files_written": 0,
    }

    async with aiohttp.ClientSession() as session:
        for nl in newsletters:
            name = nl.get("name", "")
            author = nl.get("author", "")
            rss_url = nl.get("rss_url", "")
            tier = nl.get("tier", "B")
            domains = nl.get("domains", [])

            if not name or not rss_url:
                logger.warning("Skipping newsletter with missing name/rss_url: %s", nl)
                continue

            logger.info("Ingesting newsletter: %s", name)
            stats = await ingest_newsletter(session, name, author, rss_url, tier, domains, dry_run)
            totals["newsletters_processed"] += 1
            totals["posts_found"] += stats["posts_found"]
            totals["posts_new"] += stats["posts_new"]
            totals["files_written"] += stats["files_written"]

            await asyncio.sleep(REQUEST_DELAY_S)

    logger.info(
        "Newsletter ingestion complete: %d newsletters, %d new posts, %d files",
        totals["newsletters_processed"],
        totals["posts_new"],
        totals["files_written"],
    )

    if not dry_run and totals["posts_new"] > 0:
        append_log(
            f"INGEST_SUBSTACK BATCH | "
            f"newsletters: {totals['newsletters_processed']} | "
            f"new_posts: {totals['posts_new']} | "
            f"files: {totals['files_written']}"
        )

    return totals


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest newsletter posts from configured RSS feeds (Substack, Stratechery, etc.)"
    )
    parser.add_argument("--name", help="Ingest only this newsletter (by name)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    result = asyncio.run(ingest_all(name_filter=args.name, dry_run=args.dry_run))
    if "error" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
