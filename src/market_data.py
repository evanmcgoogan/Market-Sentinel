"""
Market Data Truth Layer — unified price data with provider fallback.

Fallback chain: yfinance (1m → 1d) → stooq CSV → cached DB bars.
All price fetches are cached to asset_price_bars for offline resilience.
"""

import logging
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Ticker mapping — canonical ticker → provider-specific symbols
# ---------------------------------------------------------------------------

TICKER_MAP: Dict[str, Dict[str, str]] = {
    "SPY":  {"yfinance": "SPY",       "stooq": "SPY.US"},
    "QQQ":  {"yfinance": "QQQ",       "stooq": "QQQ.US"},
    "VIX":  {"yfinance": "^VIX",      "stooq": "VIX.US"},
    "GLD":  {"yfinance": "GLD",       "stooq": "GLD.US"},
    "SLV":  {"yfinance": "SLV",       "stooq": "SLV.US"},
    "WTI":  {"yfinance": "CL=F",      "stooq": "CL.F"},
    "COPX": {"yfinance": "COPX",      "stooq": "COPX.US"},
    "DXY":  {"yfinance": "DX-Y.NYB",  "stooq": "DXY.US"},
    "TLT":  {"yfinance": "TLT",       "stooq": "TLT.US"},
    "BTC":  {"yfinance": "BTC-USD",   "stooq": "BTC.V"},
    "ETH":  {"yfinance": "ETH-USD",   "stooq": "ETH.V"},
    "ITA":  {"yfinance": "ITA",       "stooq": "ITA.US"},
}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class PriceBar:
    ticker: str
    dt: str           # YYYY-MM-DD
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float = 0.0
    volume: Optional[float] = None
    source: str = "yfinance"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker, "bar_date": self.dt,
            "open": self.open, "high": self.high, "low": self.low,
            "close": self.close, "volume": self.volume, "source": self.source,
        }


@dataclass
class PriceSnapshot:
    ticker: str
    price: float
    timestamp: Optional[str] = None
    age_minutes: float = 0.0
    freshness: str = "unknown"   # live / delayed / stale / missing
    source: str = "yfinance"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker, "price": self.price,
            "timestamp": self.timestamp, "age_minutes": round(self.age_minutes, 2),
            "freshness": self.freshness, "source": self.source,
        }


# ---------------------------------------------------------------------------
# Cache TTLs
# ---------------------------------------------------------------------------

_INTRADAY_CACHE_TTL = 120    # 2 min for latest prices
_DAILY_CACHE_TTL = 21600     # 6 hours for daily bars


# ---------------------------------------------------------------------------
# MarketDataProvider
# ---------------------------------------------------------------------------

class MarketDataProvider:
    """
    Unified price data interface with fallback chain.

    Usage:
        mdp = MarketDataProvider(db)
        snap = mdp.get_latest_price("SPY")
        bars = mdp.get_history("SPY", days=30)
        price = mdp.get_price_at("SPY", some_datetime)
    """

    def __init__(self, db):
        self._db = db
        # In-memory cache: {ticker: (PriceSnapshot, fetched_at)}
        self._snap_cache: Dict[str, tuple] = {}

    # ── Public API ─────────────────────────────────────────────────────

    def get_latest_price(self, ticker: str) -> PriceSnapshot:
        """Best-effort current price. Falls back through providers."""
        now = _utcnow()

        # Check in-memory cache
        cached = self._snap_cache.get(ticker)
        if cached:
            snap, fetched_at = cached
            if (now - fetched_at).total_seconds() < _INTRADAY_CACHE_TTL:
                return snap

        # Try yfinance intraday
        snap = self._yf_latest(ticker)
        if snap and snap.freshness != "missing":
            self._snap_cache[ticker] = (snap, now)
            return snap

        # Try yfinance daily
        snap = self._yf_daily_latest(ticker)
        if snap and snap.freshness != "missing":
            self._snap_cache[ticker] = (snap, now)
            return snap

        # Try stooq
        snap = self._stooq_latest(ticker)
        if snap and snap.freshness != "missing":
            self._snap_cache[ticker] = (snap, now)
            return snap

        # Fall back to DB cache
        snap = self._db_latest(ticker)
        if snap:
            self._snap_cache[ticker] = (snap, now)
            return snap

        return PriceSnapshot(ticker=ticker, price=0.0, freshness="missing", source="none")

    def get_history(self, ticker: str, days: int = 30) -> List[PriceBar]:
        """Daily bars for the last N days. Caches to DB."""
        end = _utcnow()
        start = end - timedelta(days=days)
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        # Check DB cache freshness
        cached = self._db.get_price_bars(ticker, start_str, end_str)
        if cached and len(cached) >= max(1, days * 0.5):
            latest_fetch = cached[-1].get("bar_date", "")
            # If the latest bar is recent enough, use cache
            if latest_fetch >= (end - timedelta(days=2)).strftime("%Y-%m-%d"):
                return [
                    PriceBar(
                        ticker=r["ticker"], dt=r["bar_date"],
                        open=r.get("open"), high=r.get("high"),
                        low=r.get("low"), close=r["close"],
                        volume=r.get("volume"), source=r.get("source", "db_cache"),
                    )
                    for r in cached
                ]

        # Fetch fresh from yfinance
        bars = self._yf_history(ticker, days)
        if bars:
            self._cache_bars(bars)
            return bars

        # Fallback: stooq
        bars = self._stooq_history(ticker, days)
        if bars:
            self._cache_bars(bars)
            return bars

        # Last resort: whatever is in DB
        if cached:
            return [
                PriceBar(
                    ticker=r["ticker"], dt=r["bar_date"],
                    open=r.get("open"), high=r.get("high"),
                    low=r.get("low"), close=r["close"],
                    volume=r.get("volume"), source="db_cache",
                )
                for r in cached
            ]

        return []

    def get_price_at(self, ticker: str, dt: datetime) -> Optional[float]:
        """
        Nearest closing price at or before the given datetime.
        Used by the evaluator to score forecasts.
        """
        target_date = dt.strftime("%Y-%m-%d")
        # Check DB first (fast)
        start_str = (dt - timedelta(days=5)).strftime("%Y-%m-%d")
        end_str = (dt + timedelta(days=2)).strftime("%Y-%m-%d")
        bars = self._db.get_price_bars(ticker, start_str, end_str)

        if bars:
            # Find closest bar at or before target
            best = None
            for b in bars:
                if b["bar_date"] <= target_date:
                    best = b
                elif best is None:
                    # All bars are after target — use first one
                    best = b
                    break
            if best:
                return best["close"]

        # Fetch from yfinance
        try:
            import yfinance as yf
            sym = TICKER_MAP.get(ticker, {}).get("yfinance", ticker)
            fetch_start = (dt - timedelta(days=5)).strftime("%Y-%m-%d")
            fetch_end = (dt + timedelta(days=3)).strftime("%Y-%m-%d")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = yf.download(sym, start=fetch_start, end=fetch_end,
                                 interval="1d", progress=False, auto_adjust=True)
            if df is not None and not df.empty:
                # Cache these bars
                fetched_bars = self._df_to_bars(ticker, df, "yfinance")
                if fetched_bars:
                    self._cache_bars(fetched_bars)
                # Find closest
                for offset in [0, -1, -2, 1, -3, 2, -4, 3]:
                    key = (dt + timedelta(days=offset)).strftime("%Y-%m-%d")
                    for bar in fetched_bars:
                        if bar.dt == key:
                            return bar.close
        except Exception as e:
            logger.debug(f"MarketData.get_price_at yfinance failed for {ticker}: {e}")

        return None

    # ── yfinance providers ─────────────────────────────────────────────

    def _yf_latest(self, ticker: str) -> Optional[PriceSnapshot]:
        """Try yfinance 1m intraday bars for latest price."""
        try:
            import yfinance as yf
            sym = TICKER_MAP.get(ticker, {}).get("yfinance", ticker)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = yf.download(sym, period="2d", interval="1m",
                                 progress=False, auto_adjust=False, prepost=True)
            if df is None or df.empty:
                return None
            close_series = df["Close"].dropna()
            if close_series.empty:
                return None
            raw_price = close_series.iloc[-1]
            price = float(raw_price.iloc[0]) if hasattr(raw_price, "iloc") else float(raw_price)
            raw_ts = close_series.index[-1]
            ts = self._normalize_ts(raw_ts)
            now = _utcnow()
            age = (now - ts).total_seconds() / 60.0 if ts else 9999
            freshness = "live" if age <= 20 else ("delayed" if age <= 360 else "stale")
            return PriceSnapshot(
                ticker=ticker, price=round(price, 6),
                timestamp=ts.isoformat() if ts else None,
                age_minutes=max(0, age), freshness=freshness, source="yfinance_1m",
            )
        except Exception as e:
            logger.debug(f"MarketData._yf_latest failed for {ticker}: {e}")
            return None

    def _yf_daily_latest(self, ticker: str) -> Optional[PriceSnapshot]:
        """Try yfinance daily bars for latest price."""
        try:
            import yfinance as yf
            sym = TICKER_MAP.get(ticker, {}).get("yfinance", ticker)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = yf.download(sym, period="10d", interval="1d",
                                 progress=False, auto_adjust=True)
            if df is None or df.empty:
                return None
            close_series = df["Close"].dropna()
            if close_series.empty:
                return None
            raw_price = close_series.iloc[-1]
            price = float(raw_price.iloc[0]) if hasattr(raw_price, "iloc") else float(raw_price)
            raw_ts = close_series.index[-1]
            ts = self._normalize_ts(raw_ts)
            now = _utcnow()
            age = (now - ts).total_seconds() / 60.0 if ts else 9999
            freshness = "delayed" if age <= 1440 else "stale"
            return PriceSnapshot(
                ticker=ticker, price=round(price, 6),
                timestamp=ts.isoformat() if ts else None,
                age_minutes=max(0, age), freshness=freshness, source="yfinance_1d",
            )
        except Exception as e:
            logger.debug(f"MarketData._yf_daily_latest failed for {ticker}: {e}")
            return None

    def _yf_history(self, ticker: str, days: int) -> List[PriceBar]:
        """Fetch daily history from yfinance."""
        try:
            import yfinance as yf
            sym = TICKER_MAP.get(ticker, {}).get("yfinance", ticker)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = yf.download(sym, period=f"{days + 5}d", interval="1d",
                                 progress=False, auto_adjust=True)
            if df is None or df.empty:
                return []
            return self._df_to_bars(ticker, df, "yfinance")
        except Exception as e:
            logger.debug(f"MarketData._yf_history failed for {ticker}: {e}")
            return []

    # ── stooq provider ─────────────────────────────────────────────────

    def _stooq_latest(self, ticker: str) -> Optional[PriceSnapshot]:
        """Try stooq via pandas-datareader for latest daily price."""
        bars = self._stooq_history(ticker, days=5)
        if not bars:
            return None
        last = bars[-1]
        now = _utcnow()
        try:
            bar_dt = datetime.strptime(last.dt, "%Y-%m-%d")
        except Exception:
            bar_dt = now - timedelta(days=3)
        age = (now - bar_dt).total_seconds() / 60.0
        freshness = "delayed" if age <= 1440 else "stale"
        return PriceSnapshot(
            ticker=ticker, price=last.close,
            timestamp=last.dt, age_minutes=max(0, age),
            freshness=freshness, source="stooq",
        )

    def _stooq_history(self, ticker: str, days: int) -> List[PriceBar]:
        """Fetch daily history from stooq via pandas-datareader."""
        try:
            from pandas_datareader import data as pdr
            sym = TICKER_MAP.get(ticker, {}).get("stooq", ticker)
            end = _utcnow()
            start = end - timedelta(days=days + 5)
            df = pdr.DataReader(sym, "stooq", start=start.strftime("%Y-%m-%d"),
                                end=end.strftime("%Y-%m-%d"))
            if df is None or df.empty:
                return []
            # stooq returns newest-first, flip to oldest-first
            df = df.sort_index()
            bars = []
            for idx, row in df.iterrows():
                dt_str = idx.strftime("%Y-%m-%d")
                bars.append(PriceBar(
                    ticker=ticker, dt=dt_str,
                    open=_safe_float(row.get("Open")),
                    high=_safe_float(row.get("High")),
                    low=_safe_float(row.get("Low")),
                    close=_safe_float(row.get("Close")) or 0.0,
                    volume=_safe_float(row.get("Volume")),
                    source="stooq",
                ))
            return [b for b in bars if b.close > 0]
        except ImportError:
            logger.debug("pandas-datareader not installed — stooq fallback unavailable")
            return []
        except Exception as e:
            logger.debug(f"MarketData._stooq_history failed for {ticker}: {e}")
            return []

    # ── DB cache provider ──────────────────────────────────────────────

    def _db_latest(self, ticker: str) -> Optional[PriceSnapshot]:
        """Last resort: read the most recent bar from the DB cache."""
        row = self._db.get_latest_price_bar(ticker)
        if not row:
            return None
        now = _utcnow()
        try:
            bar_dt = datetime.strptime(row["bar_date"], "%Y-%m-%d")
        except Exception:
            bar_dt = now - timedelta(days=7)
        age = (now - bar_dt).total_seconds() / 60.0
        return PriceSnapshot(
            ticker=ticker, price=row["close"],
            timestamp=row["bar_date"], age_minutes=max(0, age),
            freshness="stale", source="db_cache",
        )

    # ── Helpers ────────────────────────────────────────────────────────

    def _cache_bars(self, bars: List[PriceBar]) -> None:
        """Persist bars to the DB for future offline use."""
        try:
            self._db.upsert_price_bars([b.to_dict() for b in bars])
        except Exception as e:
            logger.debug(f"MarketData._cache_bars failed: {e}")

    @staticmethod
    def _normalize_ts(ts) -> Optional[datetime]:
        """Convert pandas Timestamp / datetime to naive UTC datetime."""
        try:
            if hasattr(ts, "to_pydatetime"):
                dt = ts.to_pydatetime()
            elif isinstance(ts, datetime):
                dt = ts
            else:
                return None
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return None

    @staticmethod
    def _df_to_bars(ticker: str, df, source: str) -> List[PriceBar]:
        """Convert a pandas DataFrame to list of PriceBar."""
        bars = []
        for idx, row in df.iterrows():
            dt_str = idx.strftime("%Y-%m-%d")
            close_raw = row.get("Close")
            close_val = float(close_raw.iloc[0]) if hasattr(close_raw, "iloc") else float(close_raw) if close_raw is not None else None
            if close_val is None or close_val <= 0:
                continue
            open_raw = row.get("Open")
            high_raw = row.get("High")
            low_raw = row.get("Low")
            vol_raw = row.get("Volume")
            bars.append(PriceBar(
                ticker=ticker, dt=dt_str,
                open=_safe_float_series(open_raw),
                high=_safe_float_series(high_raw),
                low=_safe_float_series(low_raw),
                close=close_val,
                volume=_safe_float_series(vol_raw),
                source=source,
            ))
        return bars


def _safe_float(val) -> Optional[float]:
    """Safely convert to float, return None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None


def _safe_float_series(val) -> Optional[float]:
    """Safely convert a potentially Series-wrapped value to float."""
    if val is None:
        return None
    try:
        if hasattr(val, "iloc"):
            val = val.iloc[0]
        f = float(val)
        return f if f == f else None
    except (TypeError, ValueError, IndexError):
        return None
