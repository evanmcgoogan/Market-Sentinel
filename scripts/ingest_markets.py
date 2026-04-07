"""
ingest_markets.py — Snapshot prediction markets and asset prices.

Polls Polymarket (Gamma API), Kalshi, and asset price feeds, then writes
daily snapshot files to raw/markets/{platform}/YYYY-MM-DD-snapshot.md.

Usage:
    python scripts/ingest_markets.py                     # Poll all sources
    python scripts/ingest_markets.py --source polymarket  # Poll one source
    python scripts/ingest_markets.py --dry-run            # Show what would be fetched

No API key required — all endpoints are public.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any

import aiohttp

from brain_io import (
    append_log,
    is_duplicate,
    load_sources_config,
    record_hash,
    today_str,
    utcnow,
    write_raw_file,
)

logger = logging.getLogger(__name__)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)

# Categories worth tracking (skip sports, entertainment, pop culture)
SIGNAL_CATEGORIES = {
    "politics", "geopolitics", "economics", "science", "technology",
    "crypto", "finance", "ai", "regulation", "climate", "energy",
    "defense", "trade", "elections", "monetary-policy",
}


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

async def fetch_polymarket_events(
    session: aiohttp.ClientSession,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Fetch active events from Polymarket Gamma API."""
    url = f"{GAMMA_API_BASE}/events"
    params = {"active": "true", "limit": limit, "order": "volume24hr", "ascending": "false"}

    try:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                logger.warning("Polymarket events fetch failed: HTTP %d", resp.status)
                return []
            return await resp.json()
    except Exception as e:
        logger.error("Polymarket events fetch error: %s", e)
        return []


def filter_signal_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter events to those in signal-relevant categories."""
    filtered = []
    for event in events:
        # Check tags/categories
        tags = {t.get("label", "").lower() for t in event.get("tags", [])}
        categories = {c.get("label", "").lower() for c in event.get("categories", [])}
        all_labels = tags | categories

        # Also check title for relevant keywords
        title = (event.get("title") or "").lower()
        relevant_keywords = ["election", "fed", "trump", "war", "ai", "bitcoin", "rate",
                           "congress", "tariff", "china", "russia", "ukraine", "nato",
                           "recession", "inflation", "gdp", "regulation"]

        has_category = bool(all_labels & SIGNAL_CATEGORIES)
        has_keyword = any(kw in title for kw in relevant_keywords)

        if has_category or has_keyword:
            filtered.append(event)

    return filtered


def format_polymarket_event(event: dict[str, Any]) -> str:
    """Format a single Polymarket event as markdown."""
    title = event.get("title", "Unknown")
    volume = event.get("volume", 0)
    volume_24h = event.get("volume24hr", 0)
    liquidity = event.get("liquidity", 0)

    lines = [f"### {title}", ""]

    # Format markets within this event
    markets = event.get("markets", [])
    if markets:
        lines.append("| Market | Price | 24h Vol | Liquidity |")
        lines.append("|--------|-------|---------|-----------|")
        for m in markets:
            question = m.get("question", m.get("groupItemTitle", ""))
            # outcomePrices is a JSON string like "[0.65, 0.35]"
            prices = m.get("outcomePrices", "")
            if isinstance(prices, str):
                try:
                    import json
                    price_list = json.loads(prices)
                    yes_price = float(price_list[0]) if price_list else 0
                except (ValueError, IndexError):
                    yes_price = 0
            elif isinstance(prices, list):
                yes_price = float(prices[0]) if prices else 0
            else:
                yes_price = 0

            m_vol = m.get("volume24hr", 0)
            m_liq = m.get("liquidity", 0)
            lines.append(
                f"| {question[:60]} | {yes_price:.0%} | "
                f"${_compact_num(m_vol)} | ${_compact_num(m_liq)} |"
            )
        lines.append("")

    # Event-level metrics
    lines.append(f"*Total volume: ${_compact_num(volume)} | 24h: ${_compact_num(volume_24h)} | Liquidity: ${_compact_num(liquidity)}*")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

async def fetch_kalshi_events(
    session: aiohttp.ClientSession,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Fetch open events from Kalshi."""
    url = f"{KALSHI_API_BASE}/events"
    params = {"limit": limit, "status": "open"}

    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 401:
                logger.warning("Kalshi returned 401 — some endpoints may need auth")
                return []
            if resp.status != 200:
                logger.warning("Kalshi events fetch failed: HTTP %d", resp.status)
                return []
            data = await resp.json()
            return data.get("events", [])
    except Exception as e:
        logger.error("Kalshi events fetch error: %s", e)
        return []


async def fetch_kalshi_markets(
    session: aiohttp.ClientSession,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Fetch open markets from Kalshi."""
    url = f"{KALSHI_API_BASE}/markets"
    params = {"limit": limit, "status": "open"}

    try:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                logger.warning("Kalshi markets fetch failed: HTTP %d", resp.status)
                return []
            data = await resp.json()
            return data.get("markets", [])
    except Exception as e:
        logger.error("Kalshi markets fetch error: %s", e)
        return []


def format_kalshi_market(market: dict[str, Any]) -> str:
    """Format a single Kalshi market as markdown."""
    title = market.get("title", market.get("ticker", "Unknown"))
    yes_bid = market.get("yes_bid", 0)
    yes_ask = market.get("yes_ask", 0)
    mid = (yes_bid + yes_ask) / 2 if (yes_bid and yes_ask) else market.get("last_price", 0)
    volume = market.get("volume", 0)
    open_interest = market.get("open_interest", 0)

    # Kalshi prices are in cents (0-100)
    if isinstance(mid, (int, float)) and mid > 1:
        mid = mid / 100

    lines = [
        f"### {title}",
        f"- **Price**: {mid:.0%} (bid: {yes_bid}, ask: {yes_ask})",
        f"- **Volume**: {volume:,} contracts",
        f"- **Open Interest**: {open_interest:,}",
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Price feeds
# ---------------------------------------------------------------------------

async def fetch_asset_prices(session: aiohttp.ClientSession, assets: list[str]) -> dict[str, Any]:
    """Fetch current prices for a list of assets using free APIs.

    Uses Yahoo Finance v8 API (public, no key required).
    Returns dict of {ticker: {price, change, change_pct, volume}}.
    """
    prices = {}
    # Map our tickers to Yahoo Finance symbols
    yahoo_map = {
        "SPY": "SPY", "QQQ": "QQQ", "VIX": "^VIX", "TLT": "TLT",
        "GLD": "GLD", "WTI": "CL=F", "BTC": "BTC-USD", "ETH": "ETH-USD",
    }

    for ticker in assets:
        symbol = yahoo_map.get(ticker, ticker)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {"range": "1d", "interval": "1d"}

        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.warning("Price fetch failed for %s: HTTP %d", ticker, resp.status)
                    continue
                data = await resp.json()

            result = data.get("chart", {}).get("result", [])
            if not result:
                continue

            meta = result[0].get("meta", {})
            price = meta.get("regularMarketPrice", 0)
            prev_close = meta.get("previousClose") or meta.get("chartPreviousClose", 0)
            change = price - prev_close if prev_close else 0
            change_pct = (change / prev_close * 100) if prev_close else 0

            prices[ticker] = {
                "price": round(price, 2),
                "prev_close": round(prev_close, 2),
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
            }
        except Exception as e:
            logger.warning("Price fetch error for %s: %s", ticker, e)

        await asyncio.sleep(0.3)  # Rate limit courtesy

    return prices


def format_price_table(prices: dict[str, Any]) -> str:
    """Format asset prices as a markdown table."""
    lines = [
        "## Asset Prices",
        "",
        "| Ticker | Price | Change | % Change |",
        "|--------|-------|--------|----------|",
    ]
    for ticker, data in sorted(prices.items()):
        price = data["price"]
        change = data["change"]
        pct = data["change_pct"]
        sign = "+" if change >= 0 else ""
        lines.append(f"| {ticker} | ${price:,.2f} | {sign}{change:.2f} | {sign}{pct:.2f}% |")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Snapshot writing
# ---------------------------------------------------------------------------

async def ingest_polymarket(session: aiohttp.ClientSession, dry_run: bool = False) -> dict[str, int]:
    """Ingest a Polymarket snapshot."""
    stats = {"events_found": 0, "events_signal": 0, "files_written": 0}
    date = today_str()
    dedup_key = f"polymarket-{date}"

    if is_duplicate("polymarket", dedup_key):
        logger.info("Polymarket snapshot already exists for %s", date)
        return stats

    events = await fetch_polymarket_events(session)
    stats["events_found"] = len(events)

    signal_events = filter_signal_events(events)
    stats["events_signal"] = len(signal_events)

    if not signal_events:
        logger.info("No signal-relevant Polymarket events found")
        return stats

    if dry_run:
        logger.info("[DRY RUN] Would write Polymarket snapshot with %d events", len(signal_events))
        return stats

    # Build snapshot body
    body_parts = [format_polymarket_event(e) for e in signal_events]
    body = "\n---\n\n".join(body_parts)

    frontmatter = {
        "source": "polymarket",
        "collected_at": utcnow(),
        "event_count": len(signal_events),
        "total_events_fetched": len(events),
    }

    relative_path = f"raw/markets/polymarket/{date}-snapshot.md"
    write_raw_file(relative_path, frontmatter, body)
    record_hash("polymarket", dedup_key)
    stats["files_written"] = 1

    append_log(f"INGEST_POLYMARKET | events: {stats['events_signal']}/{stats['events_found']} signal")
    return stats


async def ingest_kalshi(session: aiohttp.ClientSession, dry_run: bool = False) -> dict[str, int]:
    """Ingest a Kalshi snapshot."""
    stats = {"markets_found": 0, "files_written": 0}
    date = today_str()
    dedup_key = f"kalshi-{date}"

    if is_duplicate("kalshi", dedup_key):
        logger.info("Kalshi snapshot already exists for %s", date)
        return stats

    markets = await fetch_kalshi_markets(session)
    stats["markets_found"] = len(markets)

    if not markets:
        logger.info("No Kalshi markets fetched")
        return stats

    if dry_run:
        logger.info("[DRY RUN] Would write Kalshi snapshot with %d markets", len(markets))
        return stats

    body_parts = [format_kalshi_market(m) for m in markets[:100]]  # Cap at 100
    body = "\n".join(body_parts)

    frontmatter = {
        "source": "kalshi",
        "collected_at": utcnow(),
        "market_count": len(markets),
    }

    relative_path = f"raw/markets/kalshi/{date}-snapshot.md"
    write_raw_file(relative_path, frontmatter, body)
    record_hash("kalshi", dedup_key)
    stats["files_written"] = 1

    append_log(f"INGEST_KALSHI | markets: {stats['markets_found']}")
    return stats


async def ingest_price_feeds(session: aiohttp.ClientSession, dry_run: bool = False) -> dict[str, int]:
    """Ingest asset price snapshot."""
    stats = {"assets_fetched": 0, "files_written": 0}
    date = today_str()
    dedup_key = f"prices-{date}"

    if is_duplicate("price-feed", dedup_key):
        logger.info("Price feed snapshot already exists for %s", date)
        return stats

    config = load_sources_config()
    assets = (
        config.get("markets", {})
        .get("sources", [{}])[-1]  # price-feeds is last in the list
        .get("assets", ["SPY", "QQQ", "VIX", "TLT", "GLD", "WTI", "BTC", "ETH"])
    )

    # Find the price-feeds config more robustly
    for source in config.get("markets", {}).get("sources", []):
        if source.get("name") == "price-feeds":
            assets = source.get("assets", assets)
            break

    if dry_run:
        logger.info("[DRY RUN] Would fetch prices for: %s", ", ".join(assets))
        return stats

    prices = await fetch_asset_prices(session, assets)
    stats["assets_fetched"] = len(prices)

    if not prices:
        logger.warning("No asset prices fetched")
        return stats

    body = format_price_table(prices)

    frontmatter = {
        "source": "price-feed",
        "collected_at": utcnow(),
        "asset_count": len(prices),
        "assets": list(prices.keys()),
    }

    relative_path = f"raw/markets/price-feeds/{date}.md"
    write_raw_file(relative_path, frontmatter, body)
    record_hash("price-feed", dedup_key)
    stats["files_written"] = 1

    append_log(f"INGEST_PRICES | assets: {stats['assets_fetched']} ({', '.join(prices.keys())})")
    return stats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compact_num(n: Any) -> str:
    """Format a number compactly: 1234567 → 1.2M, 45000 → 45K."""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "0"
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return f"{n:.0f}"


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def ingest_all(
    source_filter: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run market ingestion for all configured sources."""
    headers = {"Accept": "application/json", "User-Agent": "SignalHunterBrain/3.0"}

    totals: dict[str, Any] = {}

    async with aiohttp.ClientSession(headers=headers, timeout=REQUEST_TIMEOUT) as session:
        if source_filter is None or source_filter == "polymarket":
            totals["polymarket"] = await ingest_polymarket(session, dry_run)
            await asyncio.sleep(1.0)

        if source_filter is None or source_filter == "kalshi":
            totals["kalshi"] = await ingest_kalshi(session, dry_run)
            await asyncio.sleep(1.0)

        if source_filter is None or source_filter in ("price-feeds", "prices"):
            totals["price_feeds"] = await ingest_price_feeds(session, dry_run)

    if not dry_run:
        files = sum(v.get("files_written", 0) for v in totals.values() if isinstance(v, dict))
        append_log(f"INGEST_MARKETS BATCH complete | files: {files}")

    return totals


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest prediction market and asset price snapshots")
    parser.add_argument("--source", choices=["polymarket", "kalshi", "prices"], help="Ingest only this source")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched without writing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    source = args.source
    if source == "prices":
        source = "price-feeds"

    result = asyncio.run(ingest_all(source_filter=source, dry_run=args.dry_run))
    if "error" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
