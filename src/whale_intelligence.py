"""
Whale Intelligence — tracks large prediction market traders and generates
Claude-powered intelligence profiles.

Architecture: Single-pipeline discovery via Polymarket Data API.
  1. Fetch 500 most recent global trades (one API call, always has data)
  2. Filter + parse: $1K+ for trade feed, $5K+ for whale profiling
  3. Aggregate by market → Smart Money Flow
  4. Group by wallet → Top whale wallets
  5. Fetch wallet histories (parallel, 4 threads)
  6. Score + rank → Insider scoring
  7. Claude Haiku briefs for top wallets

Three intelligence products:
  • Smart Money Flow — what markets are big traders moving on
  • Whale Profiles  — who are the biggest traders, what they're doing
  • Recent Large Trades — real-time feed of $1K+ trades
"""

import concurrent.futures
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Tuple

import requests

try:
    from src.story_generator import _is_noise_market
except ImportError:
    from story_generator import _is_noise_market

logger = logging.getLogger(__name__)

# ── API endpoints ─────────────────────────────────────────────────────────────
POLY_DATA_API = "https://data-api.polymarket.com"

# Trade size thresholds (USD)
MIN_TRADE_FEED = 1_000      # Minimum for the recent trades feed + market flows
MIN_WHALE_TRADE = 5_000     # Minimum for whale wallet profiling

# Cache TTL for the full intelligence payload (seconds)
CACHE_TTL = 600  # 10 minutes — whale positions don't change every 5 min

# Keyword fragments identifying bot-dominated markets to exclude.
# Crypto binary bots and weather noise are caught here; sports/esports/pop-
# culture markets are caught by _is_noise_market() from story_generator,
# which is applied after trade parsing to keep the whale feed editorially
# consistent with the Markets page.
_EXCLUDE_TITLE = [
    "up or down", "bitcoin up", "eth up", "btc up", "sol up",
    " 5m", " 15m", " 1h ", "will it rain", "weather forecast",
]

# Keyword fragments for scoring market relevance / importance.
_RELEVANCE_KEYWORDS: Dict[str, List[str]] = {
    "geopolitics": [
        "china", "russia", "ukraine", "taiwan", "nato", "sanctions",
        "tariff", "trade war", "iran", "north korea", "india",
        "middle east", "european union", "g7", "g20", "brics",
    ],
    "politics": [
        "trump", "biden", "election", "president", "congress",
        "senate", "governor", "democrat", "republican", "vote",
        "primary", "cabinet", "impeach", "legislation", "poll",
    ],
    "conflict": [
        "war", "invasion", "military", "ceasefire", "peace deal",
        "attack", "bomb", "missile", "terror", "hostage",
        "hamas", "hezbollah", "isis", "troops",
    ],
    "markets": [
        "fed ", "interest rate", "inflation", "recession", "gdp",
        "unemployment", "s&p", "nasdaq", "bitcoin", "crypto",
        "oil price", "gold", "treasury", "default", "debt ceiling",
    ],
    "technology": [
        "ai ", "artificial intelligence", "openai", "google",
        "apple", "meta", "nvidia", "chip", "semiconductor",
        "quantum", "spacex", "launch",
    ],
}


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class WhaleTrade:
    address:      str
    condition_id: str
    market_name:  str      # from `title` field in the API
    side:         str      # "BUY" or "SELL"
    outcome:      str      # "Yes" / "No" / specific outcome label
    size:         float    # number of outcome-token shares traded
    price:        float    # USDC per share (0–1 = implied probability)
    timestamp:    datetime
    tx_hash:      str = ""
    pseudonym:    str = ""

    @property
    def usd_value(self) -> float:
        """Approximate USD spent/received: shares × price."""
        return self.size * self.price

    @property
    def implied_prob(self) -> float:
        """Price IS the implied probability on Polymarket (0–1)."""
        return self.price

    @property
    def relative_time(self) -> str:
        now  = datetime.now(timezone.utc)
        secs = max(0, int((now - self.timestamp).total_seconds()))
        if secs < 60:    return "just now"
        if secs < 3600:  return f"{secs // 60}m ago"
        if secs < 86400: return f"{secs // 3600}h ago"
        return f"{secs // 86400}d ago"

    @property
    def direction_label(self) -> str:
        return f"{self.side.title()} {self.outcome}"

    def to_feed_dict(self) -> Dict:
        """Compact dict for the recent-trades feed."""
        return {
            "address":       self.address,
            "short_address": f"{self.address[:6]}…{self.address[-4:]}",
            "pseudonym":     self.pseudonym,
            "market_name":   self.market_name,
            "side":          self.side,
            "outcome":       self.outcome,
            "usd_value":     round(self.usd_value, 2),
            "implied_prob":  round(self.implied_prob * 100, 1),
            "relative_time": self.relative_time,
            "tx_hash":       self.tx_hash,
            "direction_label": self.direction_label,
        }


@dataclass
class WhaleProfile:
    address:        str
    pseudonym:      str = ""
    total_trades:   int = 0
    total_volume:   float = 0.0
    unique_markets: int = 0
    recent_trades:  List[WhaleTrade] = field(default_factory=list)
    first_seen:     Optional[datetime] = None
    last_seen:      Optional[datetime] = None

    @property
    def short_address(self) -> str:
        return f"{self.address[:6]}…{self.address[-4:]}"

    @property
    def display_name(self) -> str:
        return self.pseudonym if self.pseudonym else self.short_address


@dataclass
class WhaleStory:
    profile:         WhaleProfile
    featured_trade:  WhaleTrade
    headline:        str
    wallet_para:     str
    trade_para:      str
    angle_para:      str
    insider_score:   float
    insider_signals: List[str]
    generated_at:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sparkline:       List[float] = field(default_factory=list)

    @property
    def score_label(self) -> str:
        if self.insider_score >= 70: return "HIGH RISK"
        if self.insider_score >= 45: return "NOTABLE"
        if self.insider_score >= 25: return "WATCH"
        return "NORMAL"

    @property
    def score_color(self) -> str:
        if self.insider_score >= 70: return "#B01C1C"
        if self.insider_score >= 45: return "#D9680F"
        if self.insider_score >= 25: return "#7A5C38"
        return "#5D6D7E"

    def to_dict(self) -> Dict:
        ft = self.featured_trade
        p  = self.profile
        return {
            "address":       p.address,
            "short_address": p.short_address,
            "display_name":  p.display_name,
            "pseudonym":     p.pseudonym,
            "total_volume":  round(p.total_volume, 2),
            "total_trades":  p.total_trades,
            "unique_markets": p.unique_markets,
            "first_seen":    p.first_seen.isoformat() if p.first_seen else None,
            "last_seen":     p.last_seen.isoformat() if p.last_seen else None,
            "featured_trade": {
                "market_name":     ft.market_name,
                "condition_id":    ft.condition_id,
                "side":            ft.side,
                "outcome":         ft.outcome,
                "size":            round(ft.size, 4),
                "price":           round(ft.price, 4),
                "usd_value":       round(ft.usd_value, 2),
                "implied_prob":    round(ft.implied_prob * 100, 1),
                "timestamp":       ft.timestamp.isoformat(),
                "relative_time":   ft.relative_time,
                "tx_hash":         ft.tx_hash,
                "direction_label": ft.direction_label,
            },
            "headline":        self.headline,
            "wallet_para":     self.wallet_para,
            "trade_para":      self.trade_para,
            "angle_para":      self.angle_para,
            "insider_score":   round(self.insider_score, 1),
            "score_label":     self.score_label,
            "score_color":     self.score_color,
            "insider_signals": self.insider_signals,
            "sparkline":       self.sparkline,
            "generated_at":    self.generated_at.isoformat(),
        }


def _whale_story_from_dict(d: Dict[str, Any]) -> "WhaleStory":
    """Reconstruct a WhaleStory shell from a cached to_dict() payload."""
    ft_raw = d.get("featured_trade") or {}
    ts_str = ft_raw.get("timestamp") or datetime.now(timezone.utc).isoformat()
    try:
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except Exception:
        ts = datetime.now(timezone.utc)

    trade = WhaleTrade(
        address=d.get("address", ""),
        condition_id=ft_raw.get("condition_id", ""),
        market_name=ft_raw.get("market_name", ""),
        side=ft_raw.get("side", "BUY"),
        outcome=ft_raw.get("outcome", "Yes"),
        size=ft_raw.get("size", 0),
        price=ft_raw.get("price", 0),
        timestamp=ts,
        tx_hash=ft_raw.get("tx_hash", ""),
        pseudonym=d.get("pseudonym", ""),
    )

    gen_str = d.get("generated_at") or d.get("_cached_at") or datetime.now(timezone.utc).isoformat()
    try:
        gen_at = datetime.fromisoformat(gen_str)
        if gen_at.tzinfo is None:
            gen_at = gen_at.replace(tzinfo=timezone.utc)
    except Exception:
        gen_at = datetime.now(timezone.utc)

    profile = WhaleProfile(
        address=d.get("address", ""),
        pseudonym=d.get("pseudonym", ""),
        total_trades=d.get("total_trades", 0),
        total_volume=d.get("total_volume", 0),
        unique_markets=d.get("unique_markets", 0),
    )
    return WhaleStory(
        profile=profile,
        featured_trade=trade,
        headline=d.get("headline", ""),
        wallet_para=d.get("wallet_para", ""),
        trade_para=d.get("trade_para", ""),
        angle_para=d.get("angle_para", ""),
        insider_score=d.get("insider_score", 0),
        insider_signals=d.get("insider_signals", []),
        generated_at=gen_at,
        sparkline=d.get("sparkline", []),
    )


# ── Smart Money Flow dataclass ────────────────────────────────────────────────

@dataclass
class MarketFlow:
    """Aggregated smart money flow for a single prediction market."""
    condition_id:   str
    market_name:    str
    total_flow:     float       # sum of all USD values
    buy_flow:       float
    sell_flow:      float
    trade_count:    int
    unique_wallets: int
    top_trade:      Optional[WhaleTrade] = None

    @property
    def net_direction(self) -> str:
        return "BUY" if self.buy_flow >= self.sell_flow else "SELL"

    @property
    def net_pct(self) -> float:
        """Percentage of flow that is buys (0–100)."""
        total = self.buy_flow + self.sell_flow
        if total == 0:
            return 50.0
        return round(100.0 * self.buy_flow / total, 1)

    def to_dict(self) -> Dict:
        top = None
        if self.top_trade:
            top = {
                "address":       self.top_trade.address,
                "short_address": f"{self.top_trade.address[:6]}…{self.top_trade.address[-4:]}",
                "pseudonym":     self.top_trade.pseudonym,
                "side":          self.top_trade.side,
                "outcome":       self.top_trade.outcome,
                "usd_value":     round(self.top_trade.usd_value, 2),
                "implied_prob":  round(self.top_trade.implied_prob * 100, 1),
                "relative_time": self.top_trade.relative_time,
            }
        return {
            "condition_id":   self.condition_id,
            "market_name":    self.market_name,
            "total_flow":     round(self.total_flow, 2),
            "buy_flow":       round(self.buy_flow, 2),
            "sell_flow":      round(self.sell_flow, 2),
            "net_direction":  self.net_direction,
            "net_pct":        self.net_pct,
            "trade_count":    self.trade_count,
            "unique_wallets": self.unique_wallets,
            "top_trade":      top,
        }


# ── Polymarket Data API client ────────────────────────────────────────────────

class PolymarketDataClient:
    """Lightweight sync HTTP client for Polymarket's public Data API."""

    TIMEOUT = 8

    def __init__(self):
        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"
        self._session.headers["User-Agent"] = "MarketSentinel/1.0"

    def _get(self, url: str, params: Dict = None) -> Any:
        try:
            r = self._session.get(url, params=params, timeout=self.TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.debug(f"Polymarket API [{url}]: {e}")
            return None

    def get_recent_trades(self, limit: int = 500) -> List[Dict]:
        """Global recent trades feed (all markets, no auth)."""
        data = self._get(f"{POLY_DATA_API}/trades", {"limit": limit})
        return data if isinstance(data, list) else []

    def get_market_trades(self, condition_id: str, limit: int = 200) -> List[Dict]:
        """Recent trades for one market by conditionId."""
        data = self._get(f"{POLY_DATA_API}/trades", {"market": condition_id, "limit": limit})
        return data if isinstance(data, list) else []

    def get_wallet_trades(self, address: str, limit: int = 200) -> List[Dict]:
        """Full trade history for a wallet address (proxy wallet)."""
        data = self._get(
            f"{POLY_DATA_API}/trades",
            {"user": address.lower(), "limit": limit},
        )
        return data if isinstance(data, list) else []


# ── Parse raw API dict → WhaleTrade ──────────────────────────────────────────

def _parse_trade(raw: Dict) -> Optional[WhaleTrade]:
    """Convert a raw Polymarket API trade dict to a WhaleTrade, or None."""
    try:
        address = (raw.get("proxyWallet") or "").lower()
        if not address or address == "0x0000000000000000000000000000000000000000":
            return None

        size  = float(raw.get("size",  0) or 0)
        price = float(raw.get("price", 0) or 0)
        if size <= 0 or price <= 0:
            return None

        title = (raw.get("title") or "").strip()
        if not title:
            return None
        title_lower = title.lower()
        if any(kw in title_lower for kw in _EXCLUDE_TITLE):
            return None

        ts = datetime.fromtimestamp(float(raw.get("timestamp", 0)), tz=timezone.utc)

        outcome   = (raw.get("outcome") or "Yes").strip()
        side      = (raw.get("side") or "BUY").upper()
        condition = raw.get("conditionId") or ""

        # Sanitize pseudonym: skip raw address-style names
        raw_name = raw.get("pseudonym") or raw.get("name") or ""
        pseudonym = ""
        if raw_name and not (raw_name.startswith("0x") and len(raw_name) > 20):
            pseudonym = raw_name.strip()

        return WhaleTrade(
            address=address,
            condition_id=condition,
            market_name=title,
            side=side,
            outcome=outcome,
            size=size,
            price=price,
            timestamp=ts,
            tx_hash=raw.get("transactionHash") or "",
            pseudonym=pseudonym,
        )
    except Exception as e:
        logger.debug(f"_parse_trade error: {e}")
        return None


# ── Insider score calculator ──────────────────────────────────────────────────

def _calc_insider_score(
    profile: WhaleProfile,
    featured: WhaleTrade,
) -> Tuple[float, List[str]]:
    """
    Score 0–100 for how suspicious / insider-like this wallet looks.
    Returns (score, human_readable_signals).
    """
    score   = 0.0
    signals: List[str] = []

    usd = featured.usd_value

    # ── Position size
    if usd >= 100_000:
        score += 35
        signals.append(f"Massive position: ${usd:,.0f}")
    elif usd >= 25_000:
        score += 25
        signals.append(f"Large position: ${usd:,.0f}")
    elif usd >= 10_000:
        score += 15
        signals.append(f"Significant position: ${usd:,.0f}")
    else:
        score += 8
        signals.append(f"Notable position: ${usd:,.0f}")

    # ── Total lifetime volume
    if profile.total_volume >= 500_000:
        score += 20
        signals.append(f"High-volume trader: ${profile.total_volume:,.0f} total")
    elif profile.total_volume >= 100_000:
        score += 12
        signals.append(f"Substantial trader: ${profile.total_volume:,.0f} total")
    elif profile.total_volume >= 20_000:
        score += 6

    # ── Market concentration (few markets = specific knowledge)
    if profile.total_trades > 0 and profile.unique_markets > 0:
        ratio = profile.total_trades / max(profile.unique_markets, 1)
        if profile.unique_markets <= 3 and profile.total_trades >= 10:
            score += 18
            signals.append(
                f"Highly concentrated: {profile.total_trades} trades on "
                f"only {profile.unique_markets} markets"
            )
        elif profile.unique_markets <= 8 and ratio >= 5:
            score += 10
            signals.append(
                f"Focused trader: avg {ratio:.1f} trades/market across "
                f"{profile.unique_markets} markets"
            )

    # ── Extreme probability bet (very confident or long-shot)
    p = featured.implied_prob
    if p >= 0.85:
        score += 10
        signals.append(f"High-conviction bet at {p:.0%} implied probability")
    elif p <= 0.15:
        score += 10
        signals.append(f"Long-shot bet at {p:.0%} implied probability")

    # ── Recency (freshness = higher relevance)
    mins_ago = (datetime.now(timezone.utc) - featured.timestamp).total_seconds() / 60
    if mins_ago < 30:
        score += 10
        signals.append(f"Very recent: {mins_ago:.0f} min ago")
    elif mins_ago < 120:
        score += 5

    # ── Has a pseudonym (real engaged trader vs disposable wallet)
    if profile.pseudonym:
        score += 4

    # ── Multi-market alignment (same wallet active on 3+ markets in batch)
    if profile.unique_markets >= 3 and profile.total_trades >= 5:
        score += 5
        signals.append(f"Multi-market player: active on {profile.unique_markets} markets")

    return min(score, 100.0), signals


def _importance_score(trade: WhaleTrade) -> float:
    """Score 0–100 for how important a trade is for intelligence synthesis.

    Factors: trade size (0-30), market relevance (0-25), recency (0-15),
    extreme probability (0-15), conviction strength (0-15).
    """
    score = 0.0

    # ── Trade size (0-30)
    usd = trade.usd_value
    if   usd >= 100_000: score += 30
    elif usd >= 50_000:  score += 25
    elif usd >= 25_000:  score += 20
    elif usd >= 10_000:  score += 15
    elif usd >= 5_000:   score += 10
    else:                score += 5

    # ── Market relevance via keyword matching (0-25)
    title_lower = trade.market_name.lower()
    best = 0
    for keywords in _RELEVANCE_KEYWORDS.values():
        hits = sum(1 for kw in keywords if kw in title_lower)
        if hits:
            best = max(best, min(25, 10 + hits * 5))
    score += best

    # ── Recency (0-15)
    try:
        mins = (datetime.now(timezone.utc) - trade.timestamp).total_seconds() / 60
    except Exception:
        mins = 999
    if   mins < 15:  score += 15
    elif mins < 60:  score += 12
    elif mins < 180: score += 8
    elif mins < 720: score += 4

    # ── Extreme probability — high conviction (0-15)
    p = trade.implied_prob
    if   p >= 0.90 or p <= 0.10: score += 15
    elif p >= 0.80 or p <= 0.20: score += 10
    elif p >= 0.70 or p <= 0.30: score += 5

    # ── Conviction strength — buying high or selling low (0-15)
    if (trade.side == "BUY" and p >= 0.75) or (trade.side == "SELL" and p <= 0.25):
        score += 15
    elif (trade.side == "BUY" and p >= 0.60) or (trade.side == "SELL" and p <= 0.40):
        score += 8

    return min(score, 100.0)


# ── WhaleBrain ────────────────────────────────────────────────────────────────

class WhaleBrain:
    """
    Single-pipeline whale discovery via Polymarket Data API.

    One API call to fetch global trades → parse → aggregate flows →
    identify top wallets → fetch histories → score → Claude briefs.
    No dependencies on alert_history, Gamma API, or blockchain RPC.
    """

    def __init__(self, api_key: str = "", db=None):
        self.api_key = api_key
        self.db      = db
        self.client  = PolymarketDataClient()

        self._cache:      Optional[Dict] = None
        self._cache_time: Optional[datetime] = None

        self._claude = None
        if api_key:
            try:
                import anthropic as _anthropic
                self._claude = _anthropic.Anthropic(api_key=api_key)
                logger.info("WhaleBrain: Claude integration active (haiku)")
            except ImportError:
                logger.warning("WhaleBrain: anthropic package not found — using template stories")

    # ── Public API ────────────────────────────────────────────────────────────

    def generate_whale_intelligence(self, limit: int = 10) -> Dict:
        """
        Main entry point.  Returns a structured intelligence payload with:
          - market_flows:   top markets by smart money volume
          - whale_profiles: top wallets with Claude stories
          - recent_trades:  chronological feed of $1K+ trades
          - stats:          aggregate metrics
          - claude_active:  whether Claude is available

        Results are cached for CACHE_TTL seconds.  Between refreshes we
        also merge with DB-persisted whale stories for 24h retention.
        """
        now = datetime.now(timezone.utc)
        cache_stale = (
            self._cache is None
            or self._cache_time is None
            or (now - self._cache_time).total_seconds() >= CACHE_TTL
        )

        # DB warmup: on first request after restart, load persisted stories
        if cache_stale and self.db:
            try:
                db_rows = self.db.get_recent_whale_stories(hours=1)
                if db_rows:
                    latest_ts_str = max(r.get("_cached_at", "") for r in db_rows)
                    if latest_ts_str:
                        latest_ts = datetime.fromisoformat(latest_ts_str)
                        if latest_ts.tzinfo is None:
                            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
                        if (now - latest_ts).total_seconds() < CACHE_TTL:
                            # Build a quick payload from cached stories
                            cached_stories = [_whale_story_from_dict(r) for r in db_rows]
                            self._cache = self._build_payload_from_stories(cached_stories, [], [])
                            self._cache_time = latest_ts
                            cache_stale = False
                            logger.info(
                                f"WhaleBrain: warmed from DB ({len(cached_stories)} stories, "
                                f"{(now - latest_ts).total_seconds():.0f}s old)"
                            )
            except Exception as exc:
                logger.debug(f"WhaleBrain DB warmup error: {exc}")

        if cache_stale:
            try:
                payload = self._compute_intelligence(limit)
            except Exception as exc:
                logger.error(f"WhaleBrain._compute_intelligence error: {exc}", exc_info=True)
                payload = self._empty_payload()

            # Persist whale stories to DB for 24h retention
            if self.db:
                try:
                    self.db.purge_old_whale_stories(hours=24)
                    for p in payload.get("whale_profiles", []):
                        self.db.save_whale_story(
                            address=p.get("address", ""),
                            condition_id=(p.get("featured_trade") or {}).get("condition_id", ""),
                            story_dict=p,
                            insider_score=p.get("insider_score", 0),
                        )
                except Exception as exc:
                    logger.debug(f"Whale DB persist error: {exc}")

            self._cache      = payload
            self._cache_time = now

        result = dict(self._cache or self._empty_payload())

        # Merge DB-cached whale stories for 24h retention
        if self.db:
            try:
                db_rows = self.db.get_recent_whale_stories(hours=24)
                existing_addrs = {p.get("address", "") for p in result.get("whale_profiles", [])}
                for row in db_rows:
                    addr = row.get("address", "")
                    if addr and addr not in existing_addrs:
                        story = _whale_story_from_dict(row)
                        result["whale_profiles"].append(story.to_dict())
                        existing_addrs.add(addr)
            except Exception:
                pass

        # Sort profiles by score and trim
        result["whale_profiles"] = sorted(
            result.get("whale_profiles", []),
            key=lambda p: p.get("insider_score", 0),
            reverse=True,
        )[:limit]

        result["claude_active"] = self._claude is not None
        result["server_time"] = now.isoformat()

        return result

    # ── Backward compat — old endpoint still calls this ───────────────────────

    def generate_whale_stories(self, limit: int = 10) -> List["WhaleStory"]:
        """Legacy entry point.  Returns WhaleStory objects for compatibility."""
        payload = self.generate_whale_intelligence(limit)
        return [
            _whale_story_from_dict(p) for p in payload.get("whale_profiles", [])
        ]

    # ── Internal computation ──────────────────────────────────────────────────

    def _compute_intelligence(self, limit: int) -> Dict:
        """Core computation pipeline. ~20-40s wall clock."""
        start = time.monotonic()

        # 1. Fetch 500 most recent global trades (ONE API call)
        raw_trades = self.client.get_recent_trades(limit=500)
        logger.info(f"WhaleBrain: fetched {len(raw_trades)} raw trades from Data API")

        # 2. Parse all trades
        all_trades = [_parse_trade(r) for r in raw_trades]
        all_trades = [t for t in all_trades if t is not None]
        # Remove sports / esports / pop-culture noise — same filter used by Markets page
        pre_filter = len(all_trades)
        all_trades = [t for t in all_trades if not _is_noise_market(t.market_name)]
        if pre_filter != len(all_trades):
            logger.info(f"WhaleBrain: filtered {pre_filter - len(all_trades)} noise markets (sports/esports/pop-culture)")
        logger.info(f"WhaleBrain: {len(all_trades)} trades passed parse filter")

        if not all_trades:
            return self._empty_payload()

        # 3. Split into two tiers
        feed_trades = [t for t in all_trades if t.usd_value >= MIN_TRADE_FEED]
        whale_trades = [t for t in all_trades if t.usd_value >= MIN_WHALE_TRADE]

        # 4. Aggregate market flows from feed-tier trades
        market_flows = self._aggregate_market_flows(feed_trades)

        # 5. Group whale-tier trades by wallet → top wallets
        by_wallet: Dict[str, List[WhaleTrade]] = defaultdict(list)
        for t in whale_trades:
            by_wallet[t.address].append(t)

        # Sort wallets by total volume in this batch
        wallet_ranking = sorted(
            by_wallet.items(),
            key=lambda kv: sum(t.usd_value for t in kv[1]),
            reverse=True,
        )[:8]  # Top 8 wallets

        logger.info(
            f"WhaleBrain: {len(feed_trades)} feed trades, "
            f"{len(whale_trades)} whale trades, "
            f"{len(wallet_ranking)} wallets to profile"
        )

        # 6. Fetch wallet histories (parallel)
        addresses = [addr for addr, _ in wallet_ranking]
        wallet_histories = self._fetch_wallet_histories(addresses)

        # 7. Build profiles, score, and rank
        scored: List[Tuple[WhaleProfile, WhaleTrade, float, List[str]]] = []
        for address, discovery_trades in wallet_ranking:
            history_trades = wallet_histories.get(address, [])
            profile = self._build_profile(address, discovery_trades, history_trades)
            featured = max(discovery_trades, key=lambda t: t.usd_value)
            score, signals = _calc_insider_score(profile, featured)
            scored.append((profile, featured, score, signals))

        # Sort by insider score
        scored.sort(key=lambda x: x[2], reverse=True)

        # 8. Generate Claude stories for top wallets
        stories: List[WhaleStory] = []
        for profile, featured, score, signals in scored[:min(limit, 6)]:
            story = self._make_story(profile, featured, score, signals)
            stories.append(story)

        # 9. Build recent trades feed (top 20 by USD value, most recent first)
        recent_feed = sorted(feed_trades, key=lambda t: t.timestamp, reverse=True)[:20]

        # 10. Score evidence trades by importance and select top 25
        evidence = sorted(feed_trades, key=_importance_score, reverse=True)[:25]

        # 11. Synthesize intelligence from all whale data
        synthesis = self._synthesize_intelligence(market_flows, stories, evidence)

        elapsed = time.monotonic() - start
        logger.info(
            f"WhaleBrain: generated {len(stories)} stories, "
            f"{len(market_flows)} market flows, synthesis in {elapsed:.1f}s"
        )

        return self._build_payload(
            market_flows, stories, recent_feed,
            synthesis=synthesis, evidence=evidence,
        )

    def _aggregate_market_flows(self, trades: List[WhaleTrade]) -> List[MarketFlow]:
        """Group trades by conditionId and compute directional flow."""
        by_market: Dict[str, List[WhaleTrade]] = defaultdict(list)
        for t in trades:
            key = t.condition_id or t.market_name
            by_market[key].append(t)

        flows: List[MarketFlow] = []
        for key, market_trades in by_market.items():
            buy_flow = sum(t.usd_value for t in market_trades if t.side == "BUY")
            sell_flow = sum(t.usd_value for t in market_trades if t.side == "SELL")
            top_trade = max(market_trades, key=lambda t: t.usd_value)
            wallets = {t.address for t in market_trades}

            flows.append(MarketFlow(
                condition_id=key,
                market_name=market_trades[0].market_name,
                total_flow=buy_flow + sell_flow,
                buy_flow=buy_flow,
                sell_flow=sell_flow,
                trade_count=len(market_trades),
                unique_wallets=len(wallets),
                top_trade=top_trade,
            ))

        # Sort by total flow, biggest first
        flows.sort(key=lambda f: f.total_flow, reverse=True)
        return flows[:12]

    def _fetch_wallet_histories(
        self, addresses: List[str]
    ) -> Dict[str, List[WhaleTrade]]:
        """Fetch wallet trade histories in parallel (4 threads, 6s timeout each)."""
        if not addresses:
            return {}

        results: Dict[str, List[WhaleTrade]] = {}

        def _fetch_one(addr: str) -> Tuple[str, List[WhaleTrade]]:
            raw = self.client.get_wallet_trades(addr, limit=200)
            trades = [_parse_trade(r) for r in raw]
            return addr, [t for t in trades if t is not None]

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                futures = {pool.submit(_fetch_one, addr): addr for addr in addresses}
                for future in concurrent.futures.as_completed(futures, timeout=15):
                    try:
                        addr, trades = future.result(timeout=6)
                        results[addr] = trades
                    except Exception:
                        results[futures[future]] = []
        except concurrent.futures.TimeoutError:
            logger.warning("WhaleBrain: wallet history fetch timed out (15s)")

        return results

    def _build_profile(
        self, address: str,
        discovery_trades: List[WhaleTrade],
        history_trades: List[WhaleTrade],
    ) -> WhaleProfile:
        """Assemble a WhaleProfile from discovery + history trades."""
        all_trades = history_trades if history_trades else discovery_trades

        total_vol      = sum(t.usd_value for t in all_trades)
        unique_markets = len({t.condition_id for t in all_trades if t.condition_id})
        timestamps     = [t.timestamp for t in all_trades]
        pseudonym      = next(
            (t.pseudonym for t in all_trades if t.pseudonym), ""
        )

        # Sort: largest USD value first
        recent = sorted(all_trades, key=lambda t: t.usd_value, reverse=True)[:20]

        return WhaleProfile(
            address=address,
            pseudonym=pseudonym,
            total_trades=len(all_trades),
            total_volume=total_vol,
            unique_markets=unique_markets,
            recent_trades=recent,
            first_seen=min(timestamps) if timestamps else None,
            last_seen=max(timestamps) if timestamps else None,
        )

    def _make_story(
        self,
        profile: WhaleProfile,
        featured: WhaleTrade,
        score: float,
        signals: List[str],
    ) -> WhaleStory:
        """Generate a WhaleStory, using Claude when available."""
        sparkline = self._get_sparkline(featured.market_name)

        if self._claude:
            try:
                gen = self._call_claude(profile, featured, score, signals)
                return WhaleStory(
                    profile=profile,
                    featured_trade=featured,
                    headline=gen.get("headline") or self._tmpl_headline(profile, featured),
                    wallet_para=gen.get("wallet_para") or self._tmpl_wallet(profile),
                    trade_para=gen.get("trade_para") or self._tmpl_trade(featured),
                    angle_para=gen.get("angle_para") or self._tmpl_angle(score, signals),
                    insider_score=score,
                    insider_signals=signals,
                    sparkline=sparkline,
                )
            except Exception as exc:
                logger.debug(f"Claude call failed for {profile.short_address}: {exc}")

        # Template fallback
        return WhaleStory(
            profile=profile,
            featured_trade=featured,
            headline=self._tmpl_headline(profile, featured),
            wallet_para=self._tmpl_wallet(profile),
            trade_para=self._tmpl_trade(featured),
            angle_para=self._tmpl_angle(score, signals),
            insider_score=score,
            insider_signals=signals,
            sparkline=sparkline,
        )

    def _call_claude(
        self,
        profile: WhaleProfile,
        trade: WhaleTrade,
        score: float,
        signals: List[str],
    ) -> Dict:
        """Call Claude Haiku and return {headline, wallet_para, trade_para, angle_para}."""
        hist_lines = []
        for t in profile.recent_trades[:5]:
            hist_lines.append(
                f"  • {t.side} {t.outcome} — \"{t.market_name[:65]}\" "
                f"| ${t.usd_value:,.0f} at {t.implied_prob:.0%} ({t.relative_time})"
            )
        history = "\n".join(hist_lines) if hist_lines else "  (no prior history available)"

        signals_str = "\n".join(f"  • {s}" for s in signals) if signals else "  • None"

        first_seen_str = ""
        if profile.first_seen:
            days_active = (datetime.now(timezone.utc) - profile.first_seen).days
            first_seen_str = f"First seen: {days_active}d ago. "

        prompt = f"""WHALE INTELLIGENCE BRIEF

Wallet: {profile.address}
Display Name: {profile.pseudonym or "Anonymous"}
{first_seen_str}Total Volume: ${profile.total_volume:,.0f} across {profile.total_trades} trades on {profile.unique_markets} unique prediction markets.

FEATURED TRADE:
  Market: "{trade.market_name}"
  Action: {trade.side} {trade.outcome}
  Size: ${trade.usd_value:,.0f} at {trade.implied_prob:.0%} implied probability
  Timing: {trade.relative_time}

TOP TRADES (by size):
{history}

INSIDER FLAGS (score {score:.0f}/100):
{signals_str}

Write a 3-section intelligence brief for a prediction market dashboard. Be specific and use the exact numbers above.

THE WALLET: 2 sentences. Who is this entity? What does their scale and trading pattern suggest about their information edge or strategy?

THE TRADE: 2 sentences. Analyze this specific position — size, implied probability, direction, and what they're betting on.

THE ANGLE: 2-3 sentences. Connect this wallet's activity to the underlying event. Could they have non-public information? What makes this suspicious or legitimate?

Return ONLY valid JSON (no markdown fences, no commentary):
{{"headline": "...", "wallet_para": "...", "trade_para": "...", "angle_para": "..."}}

Style: Bloomberg terminal analyst. Specific. Use exact dollar amounts and percentages. No hedging language."""

        resp = self._claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()

        # Strip markdown fences if model wraps in them
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1]
            if raw.lower().startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        return json.loads(raw)

    # ── Sparkline ─────────────────────────────────────────────────────────────

    def _get_sparkline(self, market_name: str) -> List[float]:
        """Attempt to retrieve a price sparkline from the DB by market name."""
        if not self.db or not market_name:
            return []
        try:
            history = self.db.get_price_history_batch([market_name], hours=24, max_points=40)
            return history.get(market_name, [])
        except Exception:
            return []

    # ── Template fallbacks (used when Claude is unavailable) ─────────────────

    def _tmpl_headline(self, profile: WhaleProfile, trade: WhaleTrade) -> str:
        return (
            f"{profile.display_name} Places ${trade.usd_value:,.0f} on "
            f"\"{trade.market_name[:55]}\""
        )

    def _tmpl_wallet(self, profile: WhaleProfile) -> str:
        return (
            f"Wallet {profile.short_address} ({profile.display_name}) has executed "
            f"${profile.total_volume:,.0f} in total prediction market volume across "
            f"{profile.unique_markets} unique markets and {profile.total_trades} recorded trades. "
            f"The scale of participation places this entity well above retail activity."
        )

    def _tmpl_trade(self, trade: WhaleTrade) -> str:
        return (
            f"The wallet placed a ${trade.usd_value:,.0f} {trade.side.lower()} on "
            f"\"{trade.market_name}\" at {trade.implied_prob:.0%} implied probability "
            f"{trade.relative_time}. "
            f"The {trade.direction_label} position represents a directional bet on the stated outcome."
        )

    def _tmpl_angle(self, score: float, signals: List[str]) -> str:
        if score >= 70:
            top = "; ".join(signals[:2])
            return (
                f"This position exhibits multiple hallmarks of potentially informed trading. "
                f"Key flags: {top}. "
                f"The combination of position size and market focus warrants close monitoring."
            )
        if score >= 45:
            top = "; ".join(signals[:2]) if signals else "position size and timing"
            return (
                f"The trading pattern is notable but not definitively suspicious. "
                f"Key observations: {top}. "
                f"The activity is consistent with a large-scale speculative positioning strategy."
            )
        return (
            "Activity appears consistent with large-scale speculative trading. "
            "No single flag rises to the level of probable insider knowledge, "
            "though the position size merits tracking."
        )

    # ── Intelligence synthesis ────────────────────────────────────────────────

    def _synthesize_intelligence(
        self,
        flows: List[MarketFlow],
        stories: List["WhaleStory"],
        evidence: List[WhaleTrade],
    ) -> Dict:
        """Produce a structured intelligence synthesis from whale data.

        Uses Claude Haiku when available; falls back to algorithmic synthesis.
        """
        if not self._claude or not evidence:
            return self._fallback_synthesis(flows, stories, evidence)

        # Format flows
        flow_lines = []
        for f in flows[:10]:
            flow_lines.append(
                f"  • {f.market_name[:70]} — "
                f"${f.total_flow:,.0f} total, {f.net_direction} {f.net_pct}%, "
                f"{f.unique_wallets} wallets, {f.trade_count} trades"
            )

        # Format indexed evidence trades
        trade_lines = []
        for i, t in enumerate(evidence[:15]):
            trade_lines.append(
                f"  [{i}] {t.side} {t.outcome} — "
                f"\"{t.market_name[:65]}\" — "
                f"${t.usd_value:,.0f} at {t.implied_prob:.0%} ({t.relative_time})"
            )

        # Format whale profiles
        whale_lines = []
        for s in stories[:6]:
            p = s.profile
            whale_lines.append(
                f"  • {p.display_name}: ${p.total_volume:,.0f} vol, "
                f"{p.unique_markets} markets, insider score {s.insider_score:.0f}/100"
            )

        flows_str   = "\n".join(flow_lines)  or "  (no flow data)"
        trades_str  = "\n".join(trade_lines) or "  (no trades)"
        whales_str  = "\n".join(whale_lines) or "  (no profiles)"

        prompt = f"""WHALE INTELLIGENCE SYNTHESIS

You are a prediction market intelligence analyst producing a Bloomberg-style briefing. Below is the latest whale trading data from Polymarket. Synthesize this into actionable intelligence.

SMART MONEY FLOWS (by market):
{flows_str}

TOP TRADES (indexed for reference):
{trades_str}

TOP WHALE PROFILES:
{whales_str}

Produce a JSON synthesis with these sections:

1. "brief": {{
     "equity_bias": "BULLISH" | "BEARISH" | "MIXED",
     "risk_appetite": "RISK-ON" | "RISK-OFF" | "NEUTRAL",
     "geopolitical_risk": "ELEVATED" | "MODERATE" | "LOW",
     "confidence": number 0-100,
     "time_horizon": "HOURS" | "DAYS" | "WEEKS",
     "synthesis": "2-3 sentence summary of what whale money is telling us. Be specific with dollar amounts and market names."
   }}

2. "lenses": [
     {{
       "title": "short title (4-8 words)",
       "body": "1-2 sentence intelligence assessment with specific numbers",
       "direction": "BULLISH" | "BEARISH" | "NEUTRAL",
       "confidence": number 0-100,
       "trade_refs": [indices from TOP TRADES]
     }}
   ] — 3 to 5 lenses, each about a different theme

3. "clusters": [
     {{
       "theme": "cluster name (e.g. 'US Election Positioning')",
       "summary": "1-2 sentence assessment",
       "direction": "BULLISH" | "BEARISH" | "MIXED",
       "total_flow": estimated dollar amount,
       "trade_refs": [indices from TOP TRADES]
     }}
   ] — 2 to 4 thematic clusters

4. "consensus": ["what whales agree on — specific", ...] — 2 to 4 items

5. "tensions": ["where whales disagree — specific", ...] — 1 to 3 items

Return ONLY valid JSON. No markdown fences. No commentary.
{{"brief":...,"lenses":[...],"clusters":[...],"consensus":[...],"tensions":[...]}}

Style: Bloomberg intelligence analyst. Specific. Use exact dollar amounts. Forward-looking implications."""

        try:
            resp = self._claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
            # Strip markdown fences if present
            if raw.startswith("```"):
                parts = raw.split("```")
                raw = parts[1]
                if raw.lower().startswith("json"):
                    raw = raw[4:]
                raw = raw.strip()

            result = json.loads(raw)

            # Validate structure — fill missing top-level keys
            if "brief" not in result or not isinstance(result["brief"], dict):
                result["brief"] = self._empty_synthesis()["brief"]
            for key in ("lenses", "clusters", "consensus", "tensions"):
                if key not in result or not isinstance(result[key], list):
                    result[key] = []

            logger.info("WhaleBrain: synthesis generated via Claude")
            return result

        except Exception as exc:
            logger.warning(f"WhaleBrain synthesis Claude call failed: {exc}")
            return self._fallback_synthesis(flows, stories, evidence)

    def _fallback_synthesis(
        self,
        flows: List[MarketFlow],
        stories: List["WhaleStory"],
        evidence: List[WhaleTrade],
    ) -> Dict:
        """Algorithmic synthesis when Claude is unavailable."""
        total_buy  = sum(f.buy_flow for f in flows)
        total_sell = sum(f.sell_flow for f in flows)
        total      = total_buy + total_sell
        buy_pct    = (total_buy / total * 100) if total > 0 else 50

        if   buy_pct >= 60: equity_bias = "BULLISH"
        elif buy_pct <= 40: equity_bias = "BEARISH"
        else:               equity_bias = "MIXED"

        large = sum(1 for t in (evidence or []) if t.usd_value >= 25_000)
        risk_appetite = "RISK-ON" if large >= 3 else "NEUTRAL"

        geo_kw = _RELEVANCE_KEYWORDS.get("conflict", [])
        geo_hits = sum(
            1 for t in (evidence or [])
            if any(kw in t.market_name.lower() for kw in geo_kw)
        )
        geo_risk = (
            "ELEVATED" if geo_hits >= 3
            else "MODERATE" if geo_hits >= 1
            else "LOW"
        )

        top_score = (
            max((s.insider_score for s in stories), default=0) if stories else 0
        )
        confidence = min(85, max(15, int(top_score * 0.7 + len(flows) * 3)))

        syn = (
            f"Whale activity shows {equity_bias.lower()} positioning with "
            f"${total:,.0f} in tracked flow across {len(flows)} markets. "
        )
        if buy_pct >= 60:
            syn += f"Buy-side dominance at {buy_pct:.0f}% suggests conviction."
        elif buy_pct <= 40:
            syn += f"Sell-side pressure at {100 - buy_pct:.0f}% suggests hedging."
        else:
            syn += "Flow is balanced — no clear directional consensus."

        # Lenses from top flows
        lenses = []
        for f in flows[:4]:
            lenses.append({
                "title": f.market_name[:55],
                "body": (
                    f"${f.total_flow:,.0f} flow from {f.unique_wallets} wallets. "
                    f"Net {f.net_direction.lower()} at {f.net_pct:.0f}%."
                ),
                "direction": "BULLISH" if f.net_direction == "BUY" else "BEARISH",
                "confidence": int(abs(f.net_pct - 50) * 1.8),
                "trade_refs": [],
            })

        # Clusters
        clusters = []
        if flows:
            clusters.append({
                "theme": "Primary Market Activity",
                "summary": (
                    f"Whale money concentrated across top "
                    f"{min(3, len(flows))} markets by flow volume."
                ),
                "direction": equity_bias,
                "total_flow": round(sum(f.total_flow for f in flows[:3]), 2),
                "trade_refs": [],
            })

        # Consensus
        buy_ct  = sum(1 for f in flows if f.net_direction == "BUY")
        sell_ct = len(flows) - buy_ct
        consensus = []
        if buy_pct >= 55:
            consensus.append(
                f"Net buying across {buy_ct}/{len(flows)} tracked markets"
            )
        elif buy_pct <= 45:
            consensus.append(
                f"Net selling across {sell_ct}/{len(flows)} tracked markets"
            )
        else:
            consensus.append(
                "Whale positioning is divided — no clear consensus"
            )

        # Tensions
        tensions = []
        buy_mkts  = [f for f in flows if f.net_direction == "BUY"]
        sell_mkts = [f for f in flows if f.net_direction == "SELL"]
        if buy_mkts and sell_mkts:
            tensions.append(
                f"Buying {buy_mkts[0].market_name[:40]} while selling "
                f"{sell_mkts[0].market_name[:40]}"
            )

        return {
            "brief": {
                "equity_bias":      equity_bias,
                "risk_appetite":    risk_appetite,
                "geopolitical_risk": geo_risk,
                "confidence":       confidence,
                "time_horizon":     "DAYS",
                "synthesis":        syn,
            },
            "lenses":    lenses,
            "clusters":  clusters,
            "consensus": consensus,
            "tensions":  tensions,
        }

    def _empty_synthesis(self) -> Dict:
        """Empty synthesis skeleton for cold-start / no-data states."""
        return {
            "brief": {
                "equity_bias":      "MIXED",
                "risk_appetite":    "NEUTRAL",
                "geopolitical_risk": "MODERATE",
                "confidence":       0,
                "time_horizon":     "DAYS",
                "synthesis":        "Awaiting whale trade data to generate intelligence synthesis.",
            },
            "lenses":    [],
            "clusters":  [],
            "consensus": [],
            "tensions":  [],
        }

    # ── Payload builders ─────────────────────────────────────────────────────

    def _build_payload(
        self,
        flows: List[MarketFlow],
        stories: List[WhaleStory],
        recent_trades: List[WhaleTrade],
        synthesis: Dict = None,
        evidence: List[WhaleTrade] = None,
    ) -> Dict:
        profiles   = [s.to_dict() for s in stories]
        total_flow = sum(f.total_flow for f in flows)
        top_score  = max((s.insider_score for s in stories), default=0)

        evidence_dicts = []
        if evidence:
            evidence_dicts = [
                {
                    **t.to_feed_dict(),
                    "idx": i,
                    "condition_id": t.condition_id,
                    "importance_score": round(_importance_score(t), 1),
                }
                for i, t in enumerate(evidence)
            ]

        return {
            "market_flows":    [f.to_dict() for f in flows],
            "whale_profiles":  profiles,
            "recent_trades":   [t.to_feed_dict() for t in recent_trades],
            "evidence_trades": evidence_dicts,
            "synthesis":       synthesis or self._empty_synthesis(),
            "stats": {
                "total_whales":      len(stories),
                "total_flow_volume": round(total_flow, 2),
                "top_insider_score": round(top_score, 1),
                "markets_with_flow": len(flows),
                "trades_scanned":    len(recent_trades),
            },
        }

    def _build_payload_from_stories(
        self,
        stories: List[WhaleStory],
        flows: List[MarketFlow],
        recent: List[WhaleTrade],
    ) -> Dict:
        """Build a payload from pre-existing WhaleStory objects (DB warmup)."""
        return self._build_payload(flows, stories, recent)

    def _empty_payload(self) -> Dict:
        return {
            "market_flows":    [],
            "whale_profiles":  [],
            "recent_trades":   [],
            "evidence_trades": [],
            "synthesis":       self._empty_synthesis(),
            "stats": {
                "total_whales":      0,
                "total_flow_volume": 0,
                "top_insider_score": 0,
                "markets_with_flow": 0,
                "trades_scanned":    0,
            },
        }
