"""
Polymarket API client.
Fetches market data from Polymarket's public API.
"""

import asyncio
import aiohttp
import logging
import math
from datetime import datetime
from typing import List, Optional, Dict, Any

from models import Market, Platform


logger = logging.getLogger(__name__)


# Polymarket CLOB API endpoints
POLYMARKET_API_BASE = "https://clob.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# Default API limits (can be overridden via config)
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_MARKETS = 500
DEFAULT_INTER_REQUEST_DELAY = 0.2
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_DELAY = 5.0


class PolymarketClient:
    """
    Async client for Polymarket's public APIs.

    Uses:
    - Gamma API for market metadata and discovery
    - CLOB API for real-time prices and order book data
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
                timeout=aiohttp.ClientTimeout(total=30)
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
                            f"Polymarket rate limit: max retries ({self.max_retries}) "
                            f"exceeded for {url}"
                        )
                        return None
                    delay = self.retry_base_delay * (2 ** _retry_count)
                    logger.warning(
                        f"Polymarket rate limited, retry {_retry_count + 1}/"
                        f"{self.max_retries} in {delay:.0f}s..."
                    )
                    await asyncio.sleep(delay)
                    return await self._request(url, params, _retry_count + 1)

                response.raise_for_status()
                return await response.json()

        except aiohttp.ClientError as e:
            logger.error(f"Polymarket API error: {e}")
            return None

    async def get_markets(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """
        Fetch active markets from Gamma API.
        Returns raw market data for filtering.
        """
        url = f"{GAMMA_API_BASE}/markets"
        params = {
            "limit": limit,
            "offset": offset,
            "active": "true",
            "closed": "false",
            "order": "volume24hr",   # Sort by most active markets first
            "ascending": "false",    # Highest volume first
        }

        data = await self._request(url, params)
        if data is None:
            return []

        return data if isinstance(data, list) else []

    async def get_all_active_markets(self) -> List[Dict]:
        """
        Fetch all active markets with pagination.
        """
        all_markets = []
        offset = 0

        while len(all_markets) < self.max_markets:
            markets = await self.get_markets(limit=self.batch_size, offset=offset)
            if not markets:
                break

            all_markets.extend(markets)
            offset += self.batch_size

            if len(markets) < self.batch_size:
                break

            # Small delay between pages
            await asyncio.sleep(self.inter_request_delay)

        return all_markets[:self.max_markets]

    async def get_market_by_id(self, condition_id: str) -> Optional[Dict]:
        """Fetch a specific market by its condition ID."""
        url = f"{GAMMA_API_BASE}/markets/{condition_id}"
        return await self._request(url)

    async def get_prices(self, token_ids: List[str]) -> Dict[str, float]:
        """
        Get current prices for token IDs from CLOB API.
        Returns map of token_id -> price (0-1 scale).
        """
        if not token_ids:
            return {}

        url = f"{POLYMARKET_API_BASE}/prices"
        params = {"token_ids": ",".join(token_ids)}

        data = await self._request(url, params)
        if data is None:
            return {}

        return data

    async def get_order_book(self, token_id: str) -> Optional[Dict]:
        """Get order book for a token (for liquidity analysis)."""
        url = f"{POLYMARKET_API_BASE}/book"
        params = {"token_id": token_id}
        return await self._request(url, params)

    def parse_market(self, raw: Dict) -> Optional[Market]:
        """
        Convert raw Polymarket API response to unified Market model.
        """
        try:
            # Extract basic info
            market_id = raw.get("condition_id") or raw.get("id", "")
            if not market_id:
                return None

            name = raw.get("question", "") or raw.get("title", "")
            if not name:
                return None

            # Parse probability - Polymarket uses "outcomePrices" or individual token prices
            probability = 50.0  # Default

            # Try outcomePrices first (format: '["0.65", "0.35"]')
            outcome_prices = raw.get("outcomePrices")
            if outcome_prices:
                if isinstance(outcome_prices, str):
                    import json
                    try:
                        prices = json.loads(outcome_prices)
                        if prices and len(prices) > 0:
                            probability = float(prices[0]) * 100
                    except (json.JSONDecodeError, ValueError):
                        pass
                elif isinstance(outcome_prices, list) and len(outcome_prices) > 0:
                    probability = float(outcome_prices[0]) * 100

            # Try tokens array for price
            tokens = raw.get("tokens", [])
            if tokens and isinstance(tokens, list):
                for token in tokens:
                    if token.get("outcome") in ["Yes", "YES", "yes", True]:
                        price = token.get("price")
                        if price is not None:
                            probability = float(price) * 100
                            break

            # Volume data
            volume_total = 0.0
            volume_24h = 0.0

            # Try different volume fields
            vol = raw.get("volume") or raw.get("volumeNum") or raw.get("volume24hr")
            if vol:
                try:
                    volume_total = float(vol)
                except (ValueError, TypeError):
                    pass

            vol_24h = raw.get("volume24hr") or raw.get("volume_24h")
            if vol_24h:
                try:
                    volume_24h = float(vol_24h)
                except (ValueError, TypeError):
                    pass

            # Liquidity
            liquidity = 0.0
            liq = raw.get("liquidity") or raw.get("liquidityNum")
            if liq:
                try:
                    liquidity = float(liq)
                except (ValueError, TypeError):
                    pass

            # End date
            end_date = None
            end_str = raw.get("endDate") or raw.get("end_date_iso")
            if end_str:
                try:
                    # Handle various date formats
                    if "T" in end_str:
                        end_date = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    else:
                        end_date = datetime.fromisoformat(end_str)
                except ValueError:
                    pass

            # Tags and category
            tags = raw.get("tags", []) or []
            if isinstance(tags, str):
                tags = [tags]

            category = raw.get("category", "") or ""
            if raw.get("groupItemTitle"):
                category = raw.get("groupItemTitle")

            # Build slug from question or ID
            slug = raw.get("slug", "") or market_id

            return Market(
                platform=Platform.POLYMARKET,
                market_id=market_id,
                slug=slug,
                name=name,
                description=raw.get("description", ""),
                category=category,
                tags=tags,
                probability=probability,
                volume_total=volume_total,
                volume_24h=volume_24h,
                liquidity=liquidity,
                end_date=end_date,
                last_updated=datetime.utcnow(),
                raw_data=raw,
            )

        except Exception as e:
            logger.error(f"Error parsing Polymarket market: {e}")
            return None

    async def fetch_markets(self) -> List[Market]:
        """
        Fetch and parse all active markets.
        Returns list of unified Market objects.
        """
        raw_markets = await self.get_all_active_markets()
        markets = []

        for raw in raw_markets:
            market = self.parse_market(raw)
            if market:
                markets.append(market)

        logger.info(f"Fetched {len(markets)} markets from Polymarket")
        return markets

    @staticmethod
    def _extract_yes_token_id(raw: Dict[str, Any]) -> Optional[str]:
        """Extract the YES token ID from a raw market payload."""
        tokens = raw.get("tokens", [])
        if not isinstance(tokens, list) or not tokens:
            return None

        for token in tokens:
            if token.get("outcome") in ["Yes", "YES", "yes", True]:
                token_id = token.get("token_id")
                if token_id:
                    return token_id

        token_id = tokens[0].get("token_id")
        return token_id if token_id else None

    async def refresh_market_prices(self, markets: List[Market]) -> List[Market]:
        """
        Refresh probabilities/volume/liquidity for an existing market list.
        Uses batched CLOB /prices lookups for low-latency updates between
        full metadata refreshes.
        """
        if not markets:
            return markets

        token_to_market: Dict[str, Market] = {}
        token_ids: List[str] = []

        for market in markets:
            token_id = self._extract_yes_token_id(market.raw_data)
            if not token_id:
                continue
            token_to_market[token_id] = market
            token_ids.append(token_id)

        if not token_ids:
            return markets

        batch_size = max(1, min(250, self.batch_size))
        updated = 0

        for i in range(0, len(token_ids), batch_size):
            batch = token_ids[i:i + batch_size]
            prices = await self.get_prices(batch)
            if not isinstance(prices, dict):
                continue

            for token_id, market in ((tid, token_to_market[tid]) for tid in batch if tid in token_to_market):
                raw_price = prices.get(token_id)
                if raw_price is None:
                    continue

                try:
                    price = float(raw_price)
                except (TypeError, ValueError):
                    continue

                if math.isnan(price):
                    continue

                # CLOB price endpoint is 0-1, normalize defensively.
                if 0.0 <= price <= 1.0:
                    market.probability = max(0.0, min(100.0, price * 100.0))
                else:
                    market.probability = max(0.0, min(100.0, price))
                updated += 1

            if i + batch_size < len(token_ids):
                await asyncio.sleep(self.inter_request_delay)

        if updated:
            logger.debug(f"Polymarket quick-refresh updated {updated} markets")
        return markets
