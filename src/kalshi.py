"""
Kalshi API client.
Fetches market data from Kalshi's public API.
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from models import Market, Platform


logger = logging.getLogger(__name__)


# Kalshi API endpoints
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Default API limits (can be overridden via config)
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_MARKETS = 500
DEFAULT_INTER_REQUEST_DELAY = 0.2
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_DELAY = 5.0


class KalshiClient:
    """
    Async client for Kalshi's public API.

    Note: Kalshi has both authenticated and public endpoints.
    This client uses only public endpoints for market data.
    """

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_markets: int = DEFAULT_MAX_MARKETS,
        inter_request_delay: float = DEFAULT_INTER_REQUEST_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_base_delay: float = DEFAULT_RETRY_BASE_DELAY,
    ):
        self._session = session
        self._owns_session = False
        self.batch_size = batch_size
        self.max_markets = max_markets
        self.inter_request_delay = inter_request_delay
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    "Accept": "application/json",
                    "User-Agent": "MarketSentinel/1.0",
                }
            )
            self._owns_session = True
        return self._session

    async def close(self):
        """Close the HTTP session if we own it."""
        if self._owns_session and self._session:
            await self._session.close()
            self._session = None

    async def _request(
        self, url: str, params: Optional[Dict] = None, _retry_count: int = 0
    ) -> Any:
        """Make an HTTP GET request with error handling and retry cap."""
        session = await self._get_session()

        try:
            async with session.get(url, params=params) as response:
                if response.status == 429:
                    if _retry_count >= self.max_retries:
                        logger.error(
                            f"Kalshi rate limit: max retries ({self.max_retries}) "
                            f"exceeded for {url}"
                        )
                        return None
                    delay = self.retry_base_delay * (2 ** _retry_count)
                    logger.warning(
                        f"Kalshi rate limited, retry {_retry_count + 1}/"
                        f"{self.max_retries} in {delay:.0f}s..."
                    )
                    await asyncio.sleep(delay)
                    return await self._request(url, params, _retry_count + 1)

                if response.status == 401:
                    logger.warning("Kalshi API returned 401 - some data may require auth")
                    return None

                response.raise_for_status()
                return await response.json()

        except aiohttp.ClientError as e:
            logger.error(f"Kalshi API error: {e}")
            return None

    async def get_events(self, limit: int = 100, cursor: Optional[str] = None) -> Dict:
        """
        Fetch events (market groups) from Kalshi.
        Events contain multiple related markets.
        """
        url = f"{KALSHI_API_BASE}/events"
        params = {
            "limit": limit,
            "status": "open",
        }
        if cursor:
            params["cursor"] = cursor

        data = await self._request(url, params)
        return data or {"events": [], "cursor": None}

    async def get_markets(
        self,
        limit: int = 100,
        cursor: Optional[str] = None,
        event_ticker: Optional[str] = None,
    ) -> Dict:
        """
        Fetch markets from Kalshi.
        Can filter by event if specified.
        """
        url = f"{KALSHI_API_BASE}/markets"
        params = {
            "limit": limit,
            "status": "open",
        }
        if cursor:
            params["cursor"] = cursor
        if event_ticker:
            params["event_ticker"] = event_ticker

        data = await self._request(url, params)
        return data or {"markets": [], "cursor": None}

    async def get_all_markets(self) -> List[Dict]:
        """
        Fetch all active markets with pagination.
        """
        all_markets = []
        cursor = None

        while len(all_markets) < self.max_markets:
            data = await self.get_markets(limit=self.batch_size, cursor=cursor)
            markets = data.get("markets", [])

            if not markets:
                break

            all_markets.extend(markets)
            cursor = data.get("cursor")

            if not cursor:
                break

            # Small delay between pages
            await asyncio.sleep(self.inter_request_delay)

        return all_markets[:self.max_markets]

    async def get_market_by_ticker(self, ticker: str) -> Optional[Dict]:
        """Fetch a specific market by its ticker."""
        url = f"{KALSHI_API_BASE}/markets/{ticker}"
        data = await self._request(url)
        return data.get("market") if data else None

    async def get_market_orderbook(self, ticker: str) -> Optional[Dict]:
        """Get order book for a market (for liquidity analysis)."""
        url = f"{KALSHI_API_BASE}/markets/{ticker}/orderbook"
        return await self._request(url)

    def parse_market(self, raw: Dict) -> Optional[Market]:
        """
        Convert raw Kalshi API response to unified Market model.
        """
        try:
            # Extract basic info
            market_id = raw.get("ticker", "")
            if not market_id:
                return None

            name = raw.get("title", "") or raw.get("subtitle", "")
            if not name:
                return None

            # Parse probability
            # Kalshi uses yes_bid/yes_ask or last_price (0-100 cents scale)
            probability = 50.0

            # Try yes_bid/yes_ask midpoint first
            yes_bid = raw.get("yes_bid")
            yes_ask = raw.get("yes_ask")
            if yes_bid is not None and yes_ask is not None:
                probability = (float(yes_bid) + float(yes_ask)) / 2

            # Fall back to last_price
            elif raw.get("last_price") is not None:
                probability = float(raw.get("last_price"))

            # Kalshi prices are in cents (0-100), so this is already %
            # But verify and clamp
            probability = max(0, min(100, probability))

            # Volume data
            volume_total = 0.0
            volume_24h = 0.0

            vol = raw.get("volume") or raw.get("volume_total")
            if vol:
                try:
                    # Kalshi volume is in contracts, estimate USD
                    # Each contract is worth $1 at settlement
                    volume_total = float(vol)
                except (ValueError, TypeError):
                    pass

            vol_24h = raw.get("volume_24h")
            if vol_24h:
                try:
                    volume_24h = float(vol_24h)
                except (ValueError, TypeError):
                    pass

            # Liquidity (open interest)
            liquidity = 0.0
            oi = raw.get("open_interest")
            if oi:
                try:
                    liquidity = float(oi)
                except (ValueError, TypeError):
                    pass

            # End date
            end_date = None
            close_time = raw.get("close_time") or raw.get("expiration_time")
            if close_time:
                try:
                    if isinstance(close_time, str):
                        end_date = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
                    elif isinstance(close_time, (int, float)):
                        end_date = datetime.fromtimestamp(close_time)
                except (ValueError, OSError):
                    pass

            # Tags and category
            tags = []
            category = raw.get("category", "") or ""

            # Event ticker can serve as category
            event_ticker = raw.get("event_ticker", "")
            if event_ticker:
                tags.append(event_ticker)

            # Series ticker
            series = raw.get("series_ticker", "")
            if series:
                tags.append(series)

            return Market(
                platform=Platform.KALSHI,
                market_id=market_id,
                slug=market_id.lower(),
                name=name,
                description=raw.get("rules_primary", "") or raw.get("description", ""),
                category=category,
                tags=tags,
                probability=probability,
                volume_total=volume_total,
                volume_24h=volume_24h,
                liquidity=liquidity,
                end_date=end_date,
                last_updated=datetime.now(timezone.utc).replace(tzinfo=None),
                raw_data=raw,
            )

        except Exception as e:
            logger.error(f"Error parsing Kalshi market: {e}")
            return None

    async def fetch_markets(self) -> List[Market]:
        """
        Fetch and parse all active markets.
        Returns list of unified Market objects.
        """
        raw_markets = await self.get_all_markets()
        markets = []

        for raw in raw_markets:
            market = self.parse_market(raw)
            if market:
                markets.append(market)

        logger.info(f"Fetched {len(markets)} markets from Kalshi")
        return markets

    async def refresh_market_prices(self, markets: List[Market]) -> List[Market]:
        """
        Refresh live fields for an existing market list.
        Kalshi lacks a clean bulk endpoint for all pricing fields, so we
        request the latest state per market with bounded concurrency.
        """
        if not markets:
            return markets

        semaphore = asyncio.Semaphore(20)
        updated = 0

        async def refresh_one(market: Market):
            nonlocal updated
            async with semaphore:
                raw = await self.get_market_by_ticker(market.market_id)
                if not raw:
                    return

                yes_bid = raw.get("yes_bid")
                yes_ask = raw.get("yes_ask")
                if yes_bid is not None and yes_ask is not None:
                    try:
                        market.probability = max(
                            0.0, min(100.0, (float(yes_bid) + float(yes_ask)) / 2.0)
                        )
                    except (TypeError, ValueError):
                        pass
                elif raw.get("last_price") is not None:
                    try:
                        market.probability = max(0.0, min(100.0, float(raw.get("last_price"))))
                    except (TypeError, ValueError):
                        pass

                vol_24h = raw.get("volume_24h")
                if vol_24h is not None:
                    try:
                        market.volume_24h = float(vol_24h)
                    except (TypeError, ValueError):
                        pass

                open_interest = raw.get("open_interest")
                if open_interest is not None:
                    try:
                        market.liquidity = float(open_interest)
                    except (TypeError, ValueError):
                        pass
                updated += 1

        await asyncio.gather(*(refresh_one(m) for m in markets), return_exceptions=True)
        if updated:
            logger.debug(f"Kalshi quick-refresh updated {updated} markets")
        return markets
