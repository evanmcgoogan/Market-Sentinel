"""
Whale Intelligence — tracks large prediction market traders and generates
Claude-powered intelligence profiles.

Primary data source: Polymarket Data API (free, public, no auth required)
  https://data-api.polymarket.com

A "whale" is a wallet that makes large ($5 000+) trades on prediction markets.
We find them by scanning the markets that have already triggered alerts in our
DB (highest signal relevance), then build full trading profiles and ask Claude
to write a three-section brief for each:

  • THE WALLET — who is this entity and what's their information edge?
  • THE TRADE  — what did they just do and why does it matter?
  • THE ANGLE  — could they be an insider? what's the narrative?
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Tuple

import requests

logger = logging.getLogger(__name__)

# ── API endpoints ─────────────────────────────────────────────────────────────
POLY_DATA_API  = "https://data-api.polymarket.com"
POLY_GAMMA_API = "https://gamma-api.polymarket.com"

# Minimum USD value of a single trade to be considered whale activity
MIN_WHALE_TRADE_USDC = 5_000

# Cache TTL for the full stories list (seconds)
CACHE_TTL = 300  # 5 minutes

# Keyword fragments that identify markets to exclude from whale discovery:
# high-frequency crypto binaries (bot-dominated) and sports (not insider-relevant)
_EXCLUDE_TITLE = [
    # High-freq crypto binary bots
    "up or down", "bitcoin up", "eth up", "btc up",
    " 5m", " 15m", " 1h ", "will it rain", "weather forecast",
    # Sports leagues and tournaments (not intelligence-relevant)
    "premier league", "la liga", "serie a", "bundesliga", "ligue 1",
    "champions league", "europa league", "fa cup", "super bowl",
    "nfl", "nba", "mlb", "nhl", " ufc ", "formula 1", "grand prix",
    "wimbledon", "world cup", "win the title", "win the league",
    "win the cup", "ballon d'or", "golden boot",
    # Sports clubs (European football dominates prediction market volumes)
    "barcelona", "real madrid", "manchester city", "manchester united",
    "liverpool", "arsenal", "chelsea", "tottenham", "atletico",
    "juventus", "inter milan", "ac milan", "napoli", "roma",
    "bayern", "borussia dortmund", "bayer leverkusen",
    "paris saint-germain", "ajax",
    # Generic sports team win/lose markets
    "vs.", " vs ", "bucks vs", "lakers vs", "celtics vs",
    # Entertainment / reality / celebrity
    "oscar", "grammy", "emmy", "box office", "kardashian",
    "taylor swift", "beyonce", "reality tv", "bachelor",
]


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
    """Reconstruct a WhaleStory shell from a cached to_dict() payload.
    Used when loading 24h-old stories from the DB — we only need the fields
    the dashboard renders, so we stub the profile/trade objects minimally.
    """
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


# ── Polymarket Data API client ────────────────────────────────────────────────

class PolymarketDataClient:
    """Lightweight sync HTTP client for Polymarket's public Data & Gamma APIs."""

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

    def get_wallet_trades(self, address: str, limit: int = 300) -> List[Dict]:
        """Full trade history for a wallet address (proxy wallet)."""
        data = self._get(
            f"{POLY_DATA_API}/trades",
            {"user": address.lower(), "limit": limit},
        )
        return data if isinstance(data, list) else []

    def get_market_info(self, market_id: str) -> Dict:
        """Fetch market metadata (conditionId, prices) by Polymarket numeric ID."""
        data = self._get(f"{POLY_GAMMA_API}/markets", {"id": market_id})
        if isinstance(data, list) and data:
            return data[0]
        return {}


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

        # Exclude high-frequency crypto binary markets
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

    return min(score, 100.0), signals


# ── WhaleBrain ────────────────────────────────────────────────────────────────

class WhaleBrain:
    """
    Orchestrates whale discovery, profiling, and Claude story generation.

    Discovery strategy (in order of relevance):
      1. Scan alert_history markets from our DB — these are the markets
         that already showed unusual movement; whale trades here are most
         likely to be connected to insider information.
      2. Fall back to the global Polymarket trade stream and filter for
         large trades on real-world events (not crypto binary bots).
    """

    def __init__(self, api_key: str = "", db=None):
        self.api_key = api_key
        self.db      = db
        self.client  = PolymarketDataClient()

        self._cache:      Optional[List[WhaleStory]] = None
        self._cache_time: Optional[datetime]         = None

        self._claude = None
        if api_key:
            try:
                import anthropic as _anthropic
                self._claude = _anthropic.Anthropic(api_key=api_key)
                logger.info("WhaleBrain: Claude integration active (haiku)")
            except ImportError:
                logger.warning("WhaleBrain: anthropic package not found — using template stories")

    # ── Public API ────────────────────────────────────────────────────────────

    def generate_whale_stories(self, limit: int = 10) -> List["WhaleStory"]:
        """
        Main entry point.  Returns up to `limit` WhaleStory objects.

        Fresh stories are computed every CACHE_TTL seconds and persisted to
        the DB.  Between refreshes — and on the Whales tab — we merge the
        fresh results with any stories from the last 24 hours so interesting
        whale activity stays visible even after the cache expires.
        """
        now = datetime.now(timezone.utc)
        cache_stale = (
            self._cache is None
            or self._cache_time is None
            or (now - self._cache_time).total_seconds() >= CACHE_TTL
        )

        if cache_stale and self.db:
            # Before triggering the 60-90s _compute scan, check whether the DB
            # already has stories fresh enough to satisfy the TTL.  This means
            # the first request after a process restart returns instantly using
            # the DB-persisted result from the previous run.
            try:
                db_rows = self.db.get_recent_whale_stories(hours=1)
                if db_rows:
                    latest_ts_str = max(r.get("_cached_at", "") for r in db_rows)
                    if latest_ts_str:
                        latest_ts = datetime.fromisoformat(latest_ts_str)
                        if latest_ts.tzinfo is None:
                            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
                        if (now - latest_ts).total_seconds() < CACHE_TTL:
                            self._cache      = [_whale_story_from_dict(r) for r in db_rows]
                            self._cache_time = latest_ts
                            cache_stale = False
                            logger.info(
                                f"WhaleBrain: warmed cache from DB "
                                f"({len(self._cache)} stories, {(now - latest_ts).total_seconds():.0f}s old)"
                            )
            except Exception as exc:
                logger.debug(f"WhaleBrain DB warmup error: {exc}")

        if cache_stale:
            try:
                fresh = self._compute(limit * 2)
            except Exception as exc:
                logger.error(f"WhaleBrain._compute error: {exc}", exc_info=True)
                fresh = []

            # Persist fresh stories to DB for 24h retention
            if self.db and fresh:
                try:
                    self.db.purge_old_whale_stories(hours=24)
                    for s in fresh:
                        self.db.save_whale_story(
                            address=s.profile.address,
                            condition_id=s.featured_trade.condition_id or "",
                            story_dict=s.to_dict(),
                            insider_score=s.insider_score,
                        )
                except Exception as exc:
                    logger.debug(f"Whale DB persist error: {exc}")

            self._cache      = fresh
            self._cache_time = now

        # Merge in-memory fresh stories with 24h DB cache
        merged_dicts: Dict[str, "WhaleStory"] = {}
        for s in (self._cache or []):
            key = f"{s.profile.address}:{s.featured_trade.condition_id}"
            merged_dicts[key] = s

        # Pull cached stories from DB that aren't already in-memory
        if self.db:
            try:
                cached_rows = self.db.get_recent_whale_stories(hours=24)
                for row in cached_rows:
                    key = f"{row.get('address','')}:{row.get('featured_trade',{}).get('condition_id','')}"
                    if key not in merged_dicts:
                        merged_dicts[key] = _whale_story_from_dict(row)
            except Exception as exc:
                logger.debug(f"Whale DB load error: {exc}")

        merged = sorted(merged_dicts.values(), key=lambda s: s.insider_score, reverse=True)
        return merged[:limit]

    # ── Internal computation ──────────────────────────────────────────────────

    def _compute(self, limit: int) -> List[WhaleStory]:
        # 1. Find large trades
        raw_trades = self._discover_whales()
        if not raw_trades:
            logger.info("WhaleBrain: no whale trades discovered")
            return []

        # 2. Group by wallet → keep all trades per wallet
        by_wallet: Dict[str, List[WhaleTrade]] = {}
        for t in raw_trades:
            by_wallet.setdefault(t.address, []).append(t)

        # 3. Build profiles + score each wallet
        scored: List[Tuple[WhaleProfile, WhaleTrade, float, List[str]]] = []
        for address, trades in by_wallet.items():
            profile  = self._build_profile(address, trades)
            featured = max(trades, key=lambda t: t.usd_value)
            score, signals = _calc_insider_score(profile, featured)
            scored.append((profile, featured, score, signals))

        # 4. Sort by insider score — most suspicious first
        scored.sort(key=lambda x: x[2], reverse=True)

        # 5. Generate stories for top N
        stories: List[WhaleStory] = []
        for profile, featured, score, signals in scored[:limit]:
            story = self._make_story(profile, featured, score, signals)
            stories.append(story)

        logger.info(
            f"WhaleBrain: generated {len(stories)} whale stories "
            f"from {len(by_wallet)} wallets"
        )
        return stories

    def _discover_whales(self) -> List[WhaleTrade]:
        """Return large trades from alert markets + global feed fallback."""
        discovered: List[WhaleTrade] = []
        seen_tx: set = set()

        def _add(t: Optional[WhaleTrade]):
            if (
                t is not None
                and t.usd_value >= MIN_WHALE_TRADE_USDC
                and t.tx_hash not in seen_tx
            ):
                seen_tx.add(t.tx_hash)
                discovered.append(t)

        # ── Strategy 1: alert_history markets (highest signal relevance)
        alert_markets = self._get_alert_market_ids(hours=72)
        logger.info(f"WhaleBrain: scanning {len(alert_markets)} alert markets for whale activity")

        for market_id, _name in alert_markets[:15]:
            try:
                info = self.client.get_market_info(market_id)
                cid  = info.get("conditionId", "")
                if not cid:
                    continue
                for raw in self.client.get_market_trades(cid, limit=200):
                    _add(_parse_trade(raw))
                time.sleep(0.1)
            except Exception as exc:
                logger.debug(f"Whale scan error (market {market_id}): {exc}")

        # ── Strategy 2: global trade stream fallback
        if len({t.address for t in discovered}) < 5:
            logger.info("WhaleBrain: falling back to global trade stream")
            try:
                for raw in self.client.get_recent_trades(limit=1000):
                    _add(_parse_trade(raw))
            except Exception as exc:
                logger.debug(f"Global whale scan error: {exc}")

        logger.info(
            f"WhaleBrain: found {len(discovered)} qualifying trades "
            f"across {len({t.address for t in discovered})} wallets"
        )
        return discovered

    def _get_alert_market_ids(self, hours: int = 72) -> List[Tuple[str, str]]:
        """Return (market_id, market_name) from recent alert_history rows."""
        if not self.db:
            return []
        try:
            cutoff = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours)).isoformat()
            with self.db._get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT DISTINCT market_id, market_name
                    FROM alert_history
                    WHERE platform = 'polymarket' AND timestamp > ?
                    ORDER BY timestamp DESC
                    LIMIT 30
                    """,
                    (cutoff,),
                ).fetchall()
            return [(r["market_id"], r["market_name"]) for r in rows]
        except Exception as exc:
            logger.debug(f"_get_alert_market_ids error: {exc}")
            return []

    def _build_profile(
        self, address: str, discovery_trades: List[WhaleTrade]
    ) -> WhaleProfile:
        """Fetch full wallet history and assemble a WhaleProfile."""
        raw_history = self.client.get_wallet_trades(address, limit=300)
        all_trades  = [_parse_trade(r) for r in raw_history]
        all_trades  = [t for t in all_trades if t is not None]

        if not all_trades:
            all_trades = discovery_trades

        total_vol      = sum(t.usd_value for t in all_trades)
        unique_markets = len({t.condition_id for t in all_trades if t.condition_id})
        timestamps     = [t.timestamp for t in all_trades]
        pseudonym      = next(
            (t.pseudonym for t in all_trades if t.pseudonym), ""
        )

        # Sort recent_trades: largest USD value first for the history summary
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
        # Build history summary (top 5 largest trades)
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
