"""
ingest_twitter.py — Poll Twitter/X accounts via SocialData.tools API.

For each configured account, fetches recent tweets and writes them as
immutable raw markdown files to raw/tweets/{handle}/YYYY-MM-DD.md.

Usage:
    python scripts/ingest_twitter.py                    # Poll all accounts
    python scripts/ingest_twitter.py --handle kaborka   # Poll one account
    python scripts/ingest_twitter.py --dry-run           # Show what would be fetched

Requires SOCIALDATA_API_KEY environment variable.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import aiohttp

from brain_io import (
    append_log,
    format_frontmatter,
    is_duplicate,
    load_sources_config,
    raw_dir,
    record_hash,
    reset_hash_cache,
    today_str,
    utcnow,
    write_raw_file,
)

logger = logging.getLogger(__name__)

SOCIALDATA_BASE = "https://api.socialdata.tools"
REQUEST_DELAY = 1.0  # Seconds between API calls to respect rate limits


# ---------------------------------------------------------------------------
# SocialData API client
# ---------------------------------------------------------------------------

async def resolve_user_id(
    session: aiohttp.ClientSession, handle: str
) -> int | None:
    """Look up a Twitter user's numeric ID from their handle."""
    url = f"{SOCIALDATA_BASE}/twitter/user/{handle}"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("id") or int(data.get("id_str", 0)) or None
            logger.warning("User lookup failed for @%s: HTTP %d", handle, resp.status)
            return None
    except Exception as e:
        logger.error("User lookup error for @%s: %s", handle, e)
        return None


async def fetch_user_tweets(
    session: aiohttp.ClientSession,
    user_id: int,
    cursor: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch a page of tweets for a user.

    Returns (tweets_list, next_cursor). next_cursor is None when no more pages.
    """
    url = f"{SOCIALDATA_BASE}/twitter/user/{user_id}/tweets"
    params: dict[str, str] = {}
    if cursor:
        params["cursor"] = cursor

    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                tweets = data.get("tweets", [])
                next_cursor = data.get("next_cursor")
                return tweets, next_cursor
            logger.warning("Tweet fetch failed for user %d: HTTP %d", user_id, resp.status)
            return [], None
    except Exception as e:
        logger.error("Tweet fetch error for user %d: %s", user_id, e)
        return [], None


# ---------------------------------------------------------------------------
# Tweet formatting
# ---------------------------------------------------------------------------

def format_tweet(tweet: dict[str, Any]) -> str:
    """Format a single tweet as markdown."""
    text = tweet.get("full_text") or tweet.get("text") or ""
    tweet_id = tweet.get("id_str", "unknown")
    created = tweet.get("tweet_created_at", "")
    screen_name = tweet.get("user", {}).get("screen_name", "unknown")

    # Engagement metrics
    likes = tweet.get("favorite_count", 0)
    retweets = tweet.get("retweet_count", 0)
    replies = tweet.get("reply_count", 0)
    quotes = tweet.get("quote_count", 0)
    views = tweet.get("views_count", 0)

    lines = [
        f"### Tweet {tweet_id}",
        f"**@{screen_name}** — {created}",
        "",
        text,
        "",
    ]

    # Metrics line
    metrics = []
    if views:
        metrics.append(f"{views:,} views")
    if likes:
        metrics.append(f"{likes:,} likes")
    if retweets:
        metrics.append(f"{retweets:,} RTs")
    if replies:
        metrics.append(f"{replies:,} replies")
    if quotes:
        metrics.append(f"{quotes:,} quotes")
    if metrics:
        lines.append(f"*{' | '.join(metrics)}*")
        lines.append("")

    # Reply context
    reply_to = tweet.get("in_reply_to_screen_name")
    if reply_to:
        lines.insert(2, f"*Replying to @{reply_to}*")
        lines.insert(3, "")

    # Quote tweet
    quoted = tweet.get("quoted_status")
    if quoted:
        qt_user = quoted.get("user", {}).get("screen_name", "unknown")
        qt_text = quoted.get("full_text") or quoted.get("text") or ""
        lines.append(f"> **@{qt_user}**: {qt_text}")
        lines.append("")

    # URLs
    urls = tweet.get("entities", {}).get("urls", [])
    if urls:
        lines.append("**Links:**")
        for u in urls:
            expanded = u.get("expanded_url") or u.get("url", "")
            if expanded:
                lines.append(f"- {expanded}")
        lines.append("")

    return "\n".join(lines)


def is_retweet(tweet: dict[str, Any]) -> bool:
    """Check if a tweet is a plain retweet (not a quote tweet)."""
    return tweet.get("retweeted_status") is not None and not tweet.get("is_quote_status", False)


def tweet_date(tweet: dict[str, Any]) -> str:
    """Extract YYYY-MM-DD from a tweet's created_at."""
    raw = tweet.get("tweet_created_at", "")
    if not raw:
        return today_str()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return today_str()


# ---------------------------------------------------------------------------
# Core ingestion logic
# ---------------------------------------------------------------------------

async def ingest_account(
    session: aiohttp.ClientSession,
    handle: str,
    tier: str,
    domains: list[str],
    dry_run: bool = False,
) -> dict[str, int]:
    """Ingest recent tweets for a single account.

    Returns stats dict: {tweets_fetched, tweets_new, tweets_skipped, files_written}.
    """
    stats = {"tweets_fetched": 0, "tweets_new": 0, "tweets_skipped": 0, "files_written": 0}

    # Resolve handle → user_id
    user_id = await resolve_user_id(session, handle)
    if not user_id:
        logger.error("Could not resolve user ID for @%s — skipping", handle)
        append_log(f"INGEST_TWITTER FAILED @{handle} | reason: user_id lookup failed")
        return stats

    # Fetch tweets (single page — most recent ~20 tweets)
    tweets, _ = await fetch_user_tweets(session, user_id)
    stats["tweets_fetched"] = len(tweets)

    if not tweets:
        logger.info("No tweets found for @%s", handle)
        return stats

    # Filter out plain retweets, group remaining by date
    original_tweets: list[dict[str, Any]] = []
    for t in tweets:
        tweet_id = t.get("id_str", "")
        if is_retweet(t):
            stats["tweets_skipped"] += 1
            continue
        if is_duplicate("twitter", tweet_id):
            stats["tweets_skipped"] += 1
            continue
        original_tweets.append(t)

    if not original_tweets:
        logger.info("No new original tweets for @%s", handle)
        return stats

    stats["tweets_new"] = len(original_tweets)

    # Group by date
    by_date: dict[str, list[dict[str, Any]]] = {}
    for t in original_tweets:
        d = tweet_date(t)
        by_date.setdefault(d, []).append(t)

    # Write one file per date
    for date_str, day_tweets in sorted(by_date.items()):
        relative_path = f"raw/tweets/{handle}/{date_str}.md"
        contains_thread = _detect_thread(day_tweets)

        if dry_run:
            logger.info("[DRY RUN] Would write %s (%d tweets)", relative_path, len(day_tweets))
            continue

        # Check if we already have a file for this date — append mode
        full_path = raw_dir() / "tweets" / handle / f"{date_str}.md"
        if full_path.exists():
            # Append new tweets to existing file
            body_parts = []
            for t in sorted(day_tweets, key=lambda x: x.get("tweet_created_at", "")):
                body_parts.append(format_tweet(t))
            append_content = "\n---\n\n".join(body_parts)
            with open(full_path, "a", encoding="utf-8") as f:
                f.write("\n---\n\n" + append_content + "\n")
            logger.info("Appended %d tweets to %s", len(day_tweets), relative_path)
        else:
            # Write new file
            frontmatter = {
                "source": "twitter",
                "handle": handle,
                "tier": tier,
                "domains": domains,
                "collected_at": utcnow(),
                "tweet_count": len(day_tweets),
                "contains_thread": contains_thread,
            }
            body_parts = []
            for t in sorted(day_tweets, key=lambda x: x.get("tweet_created_at", "")):
                body_parts.append(format_tweet(t))
            body = "\n---\n\n".join(body_parts)
            write_raw_file(relative_path, frontmatter, body)

        # Record hashes for all tweets
        for t in day_tweets:
            record_hash("twitter", t.get("id_str", ""))

        stats["files_written"] += 1

    if not dry_run:
        append_log(
            f"INGEST_TWITTER @{handle} | "
            f"fetched: {stats['tweets_fetched']} | "
            f"new: {stats['tweets_new']} | "
            f"files: {stats['files_written']}"
        )

    return stats


def _detect_thread(tweets: list[dict[str, Any]]) -> bool:
    """Check if any tweets are replies to the same user (self-thread)."""
    if len(tweets) < 2:
        return False
    for t in tweets:
        reply_to = t.get("in_reply_to_screen_name")
        user = t.get("user", {}).get("screen_name")
        if reply_to and user and reply_to.lower() == user.lower():
            return True
    return False


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def ingest_all(
    handle_filter: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run Twitter ingestion for all configured accounts (or one if filtered).

    Returns aggregate stats.
    """
    api_key = os.environ.get("SOCIALDATA_API_KEY", "")
    if not api_key:
        logger.error("SOCIALDATA_API_KEY environment variable not set")
        print("Error: Set SOCIALDATA_API_KEY environment variable", file=sys.stderr)
        return {"error": "no_api_key"}

    config = load_sources_config()
    twitter_config = config.get("twitter", {})
    accounts = twitter_config.get("accounts") or []

    if not accounts:
        logger.warning("No Twitter accounts configured in sources.yaml")
        return {"accounts_processed": 0}

    if handle_filter:
        accounts = [a for a in accounts if a.get("handle", "").lower() == handle_filter.lower()]
        if not accounts:
            logger.error("Handle @%s not found in sources.yaml", handle_filter)
            return {"error": f"handle_not_found: {handle_filter}"}

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    totals = {"accounts_processed": 0, "tweets_fetched": 0, "tweets_new": 0, "files_written": 0}

    async with aiohttp.ClientSession(
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=30),
    ) as session:
        for account in accounts:
            handle = account.get("handle", "")
            tier = account.get("tier", "C")
            domains = account.get("domains", [])

            if not handle:
                continue

            logger.info("Ingesting @%s (tier: %s)", handle, tier)
            stats = await ingest_account(session, handle, tier, domains, dry_run)

            totals["accounts_processed"] += 1
            totals["tweets_fetched"] += stats.get("tweets_fetched", 0)
            totals["tweets_new"] += stats.get("tweets_new", 0)
            totals["files_written"] += stats.get("files_written", 0)

            # Rate limit courtesy
            await asyncio.sleep(REQUEST_DELAY)

    logger.info(
        "Twitter ingestion complete: %d accounts, %d new tweets, %d files",
        totals["accounts_processed"],
        totals["tweets_new"],
        totals["files_written"],
    )

    if not dry_run:
        append_log(
            f"INGEST_TWITTER BATCH complete | "
            f"accounts: {totals['accounts_processed']} | "
            f"new_tweets: {totals['tweets_new']} | "
            f"files: {totals['files_written']}"
        )

    return totals


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest tweets from configured X/Twitter accounts")
    parser.add_argument("--handle", help="Ingest only this handle (omit @ prefix)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched without writing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    result = asyncio.run(ingest_all(handle_filter=args.handle, dry_run=args.dry_run))
    if "error" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
