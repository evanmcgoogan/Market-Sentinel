"""
SQLite database for tracking market state and history.
Lightweight persistence for price/volume history and alert cooldowns.
Extended with order book, hourly volume baselines, whale tracking, and news cache.
"""

import sqlite3
import json
import logging
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from pathlib import Path


def _utcnow() -> datetime:
    """Naive UTC datetime — drop-in for _utcnow() without the 3.12 deprecation."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


logger = logging.getLogger(__name__)
_THESIS_STOP_WORDS = frozenset([
    "the", "and", "for", "with", "that", "from", "will", "does", "what", "when",
    "are", "has", "have", "this", "into", "than", "after", "before", "while",
    "market", "markets", "thesis", "signal", "odds", "price",
])
_CATEGORY_HINTS: Dict[str, Tuple[str, ...]] = {
    "politics": ("election", "president", "senate", "congress", "trump", "biden", "governor"),
    "geopolitics": ("china", "russia", "ukraine", "taiwan", "nato", "iran", "israel"),
    "conflict": ("war", "invasion", "missile", "ceasefire", "military", "troops", "attack"),
    "technology": ("ai", "openai", "anthropic", "semiconductor", "nvidia", "chip", "crypto"),
    "markets": ("fed", "inflation", "interest rate", "gdp", "recession", "treasury", "tariff"),
}


class Database:
    """Simple SQLite wrapper for market state tracking."""

    def __init__(self, db_path: str = "market_sentinel.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript("""
                -- Market snapshots: price and volume at points in time
                CREATE TABLE IF NOT EXISTS market_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,           -- 'polymarket' or 'kalshi'
                    market_id TEXT NOT NULL,          -- Platform-specific ID
                    market_name TEXT NOT NULL,
                    probability REAL NOT NULL,        -- 0-100 scale
                    volume REAL,                      -- Total volume in USD
                    volume_24h REAL,                  -- 24h volume if available
                    liquidity REAL,                   -- Order book depth if available
                    end_date TEXT,                    -- When market resolves
                    timestamp TEXT NOT NULL,          -- ISO format
                    raw_data TEXT,                    -- JSON of full API response
                    UNIQUE(platform, market_id, timestamp)
                );

                -- Index for fast lookups
                CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
                ON market_snapshots(platform, market_id, timestamp);

                -- Alert history: track what we've alerted on
                CREATE TABLE IF NOT EXISTS alert_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    signal_score REAL NOT NULL,
                    reasons TEXT NOT NULL,            -- JSON array of reasons
                    old_probability REAL,
                    new_probability REAL,
                    timestamp TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_alerts_lookup
                ON alert_history(platform, market_id, timestamp);

                -- Market move events: all tracked notable moves, independent of alerts
                CREATE TABLE IF NOT EXISTS market_move_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    market_category TEXT DEFAULT '',
                    start_timestamp TEXT NOT NULL,
                    end_timestamp TEXT NOT NULL,
                    start_probability REAL NOT NULL,
                    end_probability REAL NOT NULL,
                    change_pp REAL NOT NULL,
                    direction INTEGER NOT NULL,       -- +1 up, -1 down
                    base_volume_24h REAL,
                    event_key TEXT NOT NULL UNIQUE,   -- deterministic hash
                    outcome_label INTEGER,            -- 1 win, 0 miss
                    outcome_magnitude REAL,
                    time_to_hit_minutes REAL,
                    outcome_checked_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_move_events_lookup
                ON market_move_events(platform, market_id, end_timestamp);

                CREATE INDEX IF NOT EXISTS idx_move_events_outcome
                ON market_move_events(outcome_label, end_timestamp);

                -- Simple key-value store for misc state
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                -- User watchlists for persistent market tracking workflows
                CREATE TABLE IF NOT EXISTS watchlists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS watchlist_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    watchlist_id INTEGER NOT NULL,
                    market_key TEXT NOT NULL,         -- platform:market_id
                    market_id TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    category TEXT,
                    notes TEXT DEFAULT '',
                    added_at TEXT NOT NULL,
                    UNIQUE(watchlist_id, market_key),
                    FOREIGN KEY(watchlist_id) REFERENCES watchlists(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_watchlist_items_lookup
                ON watchlist_items(watchlist_id, added_at DESC);

                -- Followed thesis threads for long-running event tracking
                CREATE TABLE IF NOT EXISTS thesis_threads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thesis_key TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    category TEXT,
                    topic_terms TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS thesis_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,         -- follow, signal, note, status
                    note TEXT NOT NULL,
                    payload_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(thread_id) REFERENCES thesis_threads(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_thesis_updates_thread
                ON thesis_updates(thread_id, created_at DESC);

                -- Order book snapshots: bid/ask depth over time
                CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    bid_depth REAL,                   -- Total depth on bid side (USD)
                    ask_depth REAL,                   -- Total depth on ask side (USD)
                    spread REAL,                      -- Best ask - best bid
                    best_bid REAL,                    -- Highest bid price
                    best_ask REAL,                    -- Lowest ask price
                    bid_ask_ratio REAL,               -- bid_depth / ask_depth
                    top_levels TEXT,                  -- JSON: top N price levels
                    timestamp TEXT NOT NULL,
                    UNIQUE(platform, market_id, timestamp)
                );

                CREATE INDEX IF NOT EXISTS idx_orderbook_lookup
                ON orderbook_snapshots(platform, market_id, timestamp);

                -- Hourly volume baselines: per-hour-of-day volume averages
                CREATE TABLE IF NOT EXISTS hourly_volume_baselines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    hour_utc INTEGER NOT NULL,        -- 0-23
                    avg_volume REAL NOT NULL,
                    sample_count INTEGER NOT NULL,
                    last_updated TEXT NOT NULL,
                    UNIQUE(platform, market_id, hour_utc)
                );

                -- Whale wallet tracking: known wallets and their activity
                CREATE TABLE IF NOT EXISTS whale_wallets (
                    address TEXT PRIMARY KEY,
                    label TEXT,                       -- Optional human label
                    total_trades INTEGER DEFAULT 0,
                    winning_trades INTEGER DEFAULT 0,
                    win_rate REAL DEFAULT 0.0,
                    total_volume REAL DEFAULT 0.0,    -- Total USD traded
                    first_seen TEXT,
                    last_seen TEXT,
                    is_whale INTEGER DEFAULT 0        -- 1 if meets whale criteria
                );

                CREATE INDEX IF NOT EXISTS idx_whale_volume
                ON whale_wallets(total_volume DESC);

                -- Whale trade history: individual trades by tracked wallets
                CREATE TABLE IF NOT EXISTS whale_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    address TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    market_name TEXT,
                    direction TEXT,                   -- 'buy_yes', 'buy_no', 'sell_yes', 'sell_no'
                    amount REAL,                      -- USD value
                    price REAL,                       -- Price at trade time
                    tx_hash TEXT UNIQUE,              -- Blockchain transaction hash
                    timestamp TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_whale_trades_market
                ON whale_trades(market_id, timestamp);

                CREATE INDEX IF NOT EXISTS idx_whale_trades_address
                ON whale_trades(address, timestamp);

                -- Whale story cache: persists whale stories for 24h retention
                CREATE TABLE IF NOT EXISTS whale_stories_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    address TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    story_json TEXT NOT NULL,         -- Full WhaleStory.to_dict() as JSON
                    insider_score REAL NOT NULL,
                    generated_at TEXT NOT NULL,
                    UNIQUE(address, condition_id)
                );

                CREATE INDEX IF NOT EXISTS idx_whale_stories_time
                ON whale_stories_cache(generated_at DESC);

                -- News cache: recent news articles for cross-referencing
                CREATE TABLE IF NOT EXISTS news_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    source TEXT,
                    url TEXT,
                    published_at TEXT,
                    keywords TEXT,                    -- JSON array of extracted keywords
                    fetched_at TEXT NOT NULL,
                    UNIQUE(url)
                );

                CREATE INDEX IF NOT EXISTS idx_news_keywords
                ON news_cache(fetched_at);

                -- Outlook predictions: persisted Claude Sonnet asset outlooks
                CREATE TABLE IF NOT EXISTS outlook_predictions (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id  TEXT UNIQUE NOT NULL,   -- UUID per generation
                    generated_at TEXT NOT NULL,          -- ISO UTC timestamp
                    market_regime TEXT,
                    outlook_summary TEXT,
                    dominant_themes TEXT,                -- JSON array
                    assets_json TEXT NOT NULL            -- Full per-asset predictions
                );

                CREATE INDEX IF NOT EXISTS idx_outlook_preds_time
                ON outlook_predictions(generated_at DESC);

                -- Outlook grades: actual vs predicted results per horizon
                CREATE TABLE IF NOT EXISTS outlook_grades (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id       TEXT NOT NULL,
                    horizon          TEXT NOT NULL,       -- '24h' or '48h'
                    graded_at        TEXT NOT NULL,
                    overall_score    REAL,                -- 0-1 composite
                    direction_accuracy REAL,              -- 0-1 % calls correct
                    grades_json      TEXT NOT NULL,       -- per-asset grade objects
                    reflection       TEXT DEFAULT '',     -- Claude's analysis
                    UNIQUE(session_id, horizon)
                );

                CREATE INDEX IF NOT EXISTS idx_outlook_grades_time
                ON outlook_grades(graded_at DESC);
            """)
            self._ensure_schema_updates(conn)

    def _ensure_schema_updates(self, conn: sqlite3.Connection):
        """Apply additive schema migrations for older databases."""
        # alert_history outcome labeling fields
        alert_columns = {row["name"] for row in conn.execute("PRAGMA table_info(alert_history)").fetchall()}
        alert_migrations: List[Tuple[str, str]] = [
            ("signal_types", "ALTER TABLE alert_history ADD COLUMN signal_types TEXT"),
            ("market_category", "ALTER TABLE alert_history ADD COLUMN market_category TEXT"),
            ("outcome_label", "ALTER TABLE alert_history ADD COLUMN outcome_label INTEGER"),
            ("outcome_magnitude", "ALTER TABLE alert_history ADD COLUMN outcome_magnitude REAL"),
            ("time_to_hit_minutes", "ALTER TABLE alert_history ADD COLUMN time_to_hit_minutes REAL"),
            ("outcome_checked_at", "ALTER TABLE alert_history ADD COLUMN outcome_checked_at TEXT"),
        ]
        for column, ddl in alert_migrations:
            if column not in alert_columns:
                conn.execute(ddl)

        thesis_columns = {row["name"] for row in conn.execute("PRAGMA table_info(thesis_threads)").fetchall()}
        thesis_migrations: List[Tuple[str, str]] = [
            ("topic_terms", "ALTER TABLE thesis_threads ADD COLUMN topic_terms TEXT"),
        ]
        for column, ddl in thesis_migrations:
            if column not in thesis_columns:
                conn.execute(ddl)

    @contextmanager
    def _get_conn(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        # Apply per-connection performance settings.
        # synchronous=NORMAL is safe with WAL mode and significantly reduces fsync overhead.
        # busy_timeout is a belt-and-suspenders fallback beyond the connect timeout.
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=10000")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise  # Re-raise so callers know the operation failed
        finally:
            conn.close()

    # ==================== Market Snapshots ====================

    def save_snapshot(
        self,
        platform: str,
        market_id: str,
        market_name: str,
        probability: float,
        volume: Optional[float] = None,
        volume_24h: Optional[float] = None,
        liquidity: Optional[float] = None,
        end_date: Optional[str] = None,
        raw_data: Optional[Dict] = None,
    ):
        """Save a market snapshot."""
        timestamp = _utcnow().isoformat()

        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO market_snapshots
                (platform, market_id, market_name, probability, volume, volume_24h,
                 liquidity, end_date, timestamp, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                platform, market_id, market_name, probability, volume, volume_24h,
                liquidity, end_date, timestamp,
                json.dumps(raw_data) if raw_data else None
            ))

    def get_recent_snapshots(
        self,
        platform: str,
        market_id: str,
        minutes: int = 60,
    ) -> List[Dict[str, Any]]:
        """Get snapshots from the last N minutes."""
        cutoff = (_utcnow() - timedelta(minutes=minutes)).isoformat()

        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM market_snapshots
                WHERE platform = ? AND market_id = ? AND timestamp > ?
                ORDER BY timestamp ASC
            """, (platform, market_id, cutoff)).fetchall()

        return [dict(row) for row in rows]

    def get_latest_snapshot(
        self,
        platform: str,
        market_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent snapshot for a market."""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT * FROM market_snapshots
                WHERE platform = ? AND market_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (platform, market_id)).fetchone()

        return dict(row) if row else None

    def get_baseline_volume(
        self,
        platform: str,
        market_id: str,
        hours: int = 24,
    ) -> Optional[float]:
        """
        Calculate average 24h volume over the baseline period.
        Returns the mean of all recorded volume_24h values in the window,
        giving a more stable baseline than a single snapshot.
        """
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()

        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT AVG(volume_24h) as avg_volume, COUNT(*) as cnt
                FROM market_snapshots
                WHERE platform = ? AND market_id = ? AND timestamp > ?
                  AND volume_24h IS NOT NULL AND volume_24h > 0
            """, (platform, market_id, cutoff)).fetchone()

        if not row or row["cnt"] == 0:
            return None

        return row["avg_volume"]

    # ==================== Alert History ====================

    def record_alert(
        self,
        platform: str,
        market_id: str,
        market_name: str,
        signal_score: float,
        reasons: List[str],
        old_probability: Optional[float],
        new_probability: Optional[float],
        signal_types: Optional[List[str]] = None,
        market_category: Optional[str] = None,
    ):
        """Record that we sent an alert."""
        timestamp = _utcnow().isoformat()

        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO alert_history
                (platform, market_id, market_name, signal_score, reasons,
                 old_probability, new_probability, timestamp, signal_types, market_category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                platform, market_id, market_name, signal_score,
                json.dumps(reasons), old_probability, new_probability, timestamp,
                json.dumps(signal_types or []), market_category or ""
            ))

    def get_last_alert_time(
        self,
        platform: str,
        market_id: str,
    ) -> Optional[datetime]:
        """Get when we last alerted on this market."""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT timestamp FROM alert_history
                WHERE platform = ? AND market_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (platform, market_id)).fetchone()

        if row:
            return datetime.fromisoformat(row["timestamp"])
        return None

    def count_recent_alerts(self, minutes: int = 60) -> int:
        """Count alerts in the last N minutes."""
        cutoff = (_utcnow() - timedelta(minutes=minutes)).isoformat()

        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT COUNT(*) as count FROM alert_history
                WHERE timestamp > ?
            """, (cutoff,)).fetchone()

        return row["count"] if row else 0

    # ==================== State Store ====================

    def set_state(self, key: str, value: Any):
        """Store arbitrary state."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO state (key, value, updated_at)
                VALUES (?, ?, ?)
            """, (key, json.dumps(value), _utcnow().isoformat()))

    def get_state(self, key: str, default: Any = None) -> Any:
        """Retrieve stored state."""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT value FROM state WHERE key = ?
            """, (key,)).fetchone()

        if row:
            return json.loads(row["value"])
        return default

    @staticmethod
    def _topic_terms(text: str, limit: int = 8) -> List[str]:
        import re
        words = []
        tokens = re.findall(r"[a-zA-Z]{3,}", (text or "").lower())
        seen = set()
        for token in tokens:
            if token in _THESIS_STOP_WORDS:
                continue
            if token in seen:
                continue
            seen.add(token)
            words.append(token)
            if len(words) >= limit:
                break
        return words

    @staticmethod
    def _infer_market_category(name: str) -> str:
        nl = (name or "").lower()
        for category, hints in _CATEGORY_HINTS.items():
            if any(h in nl for h in hints):
                return category
        return "other"

    @staticmethod
    def _safe_json_list(raw: Any) -> List[Any]:
        if raw is None:
            return []
        if isinstance(raw, list):
            return raw
        if not isinstance(raw, str):
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []

    @staticmethod
    def _safe_json_dict(raw: Any) -> Dict[str, Any]:
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str):
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    # ==================== Watchlists ====================

    def ensure_watchlist(self, name: str = "Default") -> int:
        """Create watchlist if missing, return watchlist id."""
        now = _utcnow().isoformat()
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO watchlists (name, created_at, updated_at)
                VALUES (?, ?, ?)
                """,
                (name, now, now),
            )
            row = conn.execute(
                "SELECT id FROM watchlists WHERE name = ?",
                (name,),
            ).fetchone()
        return int(row["id"])

    def add_watchlist_item(
        self,
        watchlist_name: str,
        market_id: str,
        market_name: str,
        platform: str,
        category: str = "",
        notes: str = "",
    ) -> bool:
        """Add market to watchlist; idempotent on duplicate."""
        watchlist_id = self.ensure_watchlist(watchlist_name)
        now = _utcnow().isoformat()
        market_key = f"{platform}:{market_id}"

        with self._get_conn() as conn:
            rows = conn.execute(
                """
                INSERT OR IGNORE INTO watchlist_items
                (watchlist_id, market_key, market_id, market_name, platform, category, notes, added_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (watchlist_id, market_key, market_id, market_name, platform, category, notes, now),
            ).rowcount
            conn.execute(
                "UPDATE watchlists SET updated_at = ? WHERE id = ?",
                (now, watchlist_id),
            )
        return rows > 0

    def get_watchlists(self) -> List[Dict[str, Any]]:
        """Return all watchlists and items."""
        with self._get_conn() as conn:
            watchlists = conn.execute(
                "SELECT id, name, created_at, updated_at FROM watchlists ORDER BY updated_at DESC"
            ).fetchall()

            result: List[Dict[str, Any]] = []
            for wl in watchlists:
                items = conn.execute(
                    """
                    SELECT id, market_id, market_name, platform, category, notes, added_at
                    FROM watchlist_items
                    WHERE watchlist_id = ?
                    ORDER BY added_at DESC
                    """,
                    (wl["id"],),
                ).fetchall()
                result.append({
                    "id": wl["id"],
                    "name": wl["name"],
                    "created_at": wl["created_at"],
                    "updated_at": wl["updated_at"],
                    "items": [dict(i) for i in items],
                })
        return result

    def get_watchlists_enriched(
        self,
        max_items_per_watchlist: int = 40,
        move_window_hours: int = 24,
        signal_window_hours: int = 72,
    ) -> List[Dict[str, Any]]:
        """
        Return watchlists enriched with live context:
        - latest probability and 24h probability delta
        - latest signal score/time/type
        - per-item priority score for decision queue ordering

        Uses 3 batch queries (one per data source) instead of N×3 individual
        queries so performance is O(1) in the number of watchlist items.
        """
        now = _utcnow()
        move_cutoff   = (now - timedelta(hours=move_window_hours)).isoformat()
        signal_cutoff = (now - timedelta(hours=signal_window_hours)).isoformat()

        with self._get_conn() as conn:
            watchlists = conn.execute(
                "SELECT id, name, created_at, updated_at FROM watchlists ORDER BY updated_at DESC"
            ).fetchall()

            if not watchlists:
                return []

            # ── 1. Collect all items across every watchlist in one query ──────
            wl_ids     = [wl["id"] for wl in watchlists]
            id_ph      = ",".join("?" * len(wl_ids))
            all_items  = conn.execute(
                f"""
                SELECT id, watchlist_id, market_id, market_name, platform,
                       category, notes, added_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY watchlist_id ORDER BY added_at DESC
                       ) AS rn
                FROM watchlist_items
                WHERE watchlist_id IN ({id_ph})
                """,
                wl_ids,
            ).fetchall()
            # Respect per-watchlist limit
            all_items = [r for r in all_items if r["rn"] <= max_items_per_watchlist]

            if not all_items:
                # Return watchlists with empty items lists
                return [
                    {
                        "id": wl["id"], "name": wl["name"],
                        "created_at": wl["created_at"], "updated_at": wl["updated_at"],
                        "item_count": 0, "active_signals_24h": 0,
                        "stale_items": 0, "avg_priority": 0.0, "items": [],
                    }
                    for wl in watchlists
                ]

            # ── 2. Build composite keys for batch lookups ─────────────────────
            mk_list = list({f"{r['platform']}:{r['market_id']}" for r in all_items})
            mk_ph   = ",".join("?" * len(mk_list))

            # ── 3a. Latest snapshot per (platform, market_id) ─────────────────
            latest_rows = conn.execute(
                f"""
                SELECT s.platform, s.market_id, s.probability, s.timestamp
                FROM market_snapshots s
                JOIN (
                    SELECT platform, market_id, MAX(timestamp) AS max_ts
                    FROM market_snapshots
                    WHERE (platform || ':' || market_id) IN ({mk_ph})
                    GROUP BY platform, market_id
                ) best ON s.platform = best.platform
                      AND s.market_id = best.market_id
                      AND s.timestamp = best.max_ts
                """,
                mk_list,
            ).fetchall()
            latest_map: Dict[str, Any] = {
                f"{r['platform']}:{r['market_id']}": r for r in latest_rows
            }

            # ── 3b. Baseline snapshot before move_cutoff ──────────────────────
            baseline_rows = conn.execute(
                f"""
                SELECT s.platform, s.market_id, s.probability, s.timestamp
                FROM market_snapshots s
                JOIN (
                    SELECT platform, market_id, MAX(timestamp) AS max_ts
                    FROM market_snapshots
                    WHERE (platform || ':' || market_id) IN ({mk_ph})
                      AND timestamp <= ?
                    GROUP BY platform, market_id
                ) best ON s.platform = best.platform
                      AND s.market_id = best.market_id
                      AND s.timestamp = best.max_ts
                """,
                [*mk_list, move_cutoff],
            ).fetchall()
            baseline_map: Dict[str, Any] = {
                f"{r['platform']}:{r['market_id']}": r for r in baseline_rows
            }

            # ── 3c. Latest alert after signal_cutoff ──────────────────────────
            alert_rows = conn.execute(
                f"""
                SELECT a.platform, a.market_id, a.signal_score, a.signal_types, a.timestamp
                FROM alert_history a
                JOIN (
                    SELECT platform, market_id, MAX(timestamp) AS max_ts
                    FROM alert_history
                    WHERE (platform || ':' || market_id) IN ({mk_ph})
                      AND timestamp > ?
                    GROUP BY platform, market_id
                ) best ON a.platform = best.platform
                      AND a.market_id = best.market_id
                      AND a.timestamp = best.max_ts
                """,
                [*mk_list, signal_cutoff],
            ).fetchall()
            alert_map: Dict[str, Any] = {
                f"{r['platform']}:{r['market_id']}": r for r in alert_rows
            }

        # ── 4. Group items by watchlist and enrich in Python ──────────────────
        items_by_wl: Dict[int, List[Any]] = {}
        for r in all_items:
            items_by_wl.setdefault(int(r["watchlist_id"]), []).append(r)

        out: List[Dict[str, Any]] = []
        for wl in watchlists:
            wl_items = items_by_wl.get(int(wl["id"]), [])

            enriched_items: List[Dict[str, Any]] = []
            active_signals_24h = 0
            stale_items        = 0
            hotness_total      = 0.0

            for item in wl_items:
                mk = f"{item['platform']}:{item['market_id']}"

                latest   = latest_map.get(mk)
                baseline = baseline_map.get(mk)
                alert    = alert_map.get(mk)

                latest_prob = float(latest["probability"]) if latest else None
                delta_24h   = None
                if latest and baseline:
                    delta_24h = round(
                        float(latest["probability"]) - float(baseline["probability"]), 3
                    )

                last_signal_score = (
                    float(alert["signal_score"])
                    if alert and alert["signal_score"] is not None
                    else None
                )
                last_signal_types = [
                    str(s) for s in self._safe_json_list(
                        alert["signal_types"] if alert else "[]"
                    )
                ]
                last_signal_at = alert["timestamp"] if alert else None

                hours_since_signal = None
                if last_signal_at:
                    try:
                        last_dt = datetime.fromisoformat(last_signal_at)
                        hours_since_signal = max(
                            0.0, (now - last_dt).total_seconds() / 3600.0
                        )
                    except ValueError:
                        pass

                hours_since_snapshot = 999.0
                if latest:
                    try:
                        latest_dt = datetime.fromisoformat(latest["timestamp"])
                        hours_since_snapshot = max(
                            0.0, (now - latest_dt).total_seconds() / 3600.0
                        )
                    except ValueError:
                        pass

                if hours_since_snapshot > 12:
                    stale_items += 1
                if last_signal_at and hours_since_signal is not None and hours_since_signal <= 24:
                    active_signals_24h += 1

                move_score  = min(30.0, abs(delta_24h or 0.0) * 3.0)
                sig_score   = (last_signal_score or 0.0) * 0.8
                freshness   = (
                    max(0.0, 20.0 - hours_since_signal * 1.2)
                    if hours_since_signal is not None else 0.0
                )
                hotness = round(move_score + sig_score + freshness, 2)
                hotness_total += hotness

                enriched_items.append({
                    **{k: item[k] for k in item.keys() if k != "rn"},
                    "latest_probability":  round(latest_prob, 3) if latest_prob is not None else None,
                    "latest_snapshot_at":  latest["timestamp"] if latest else None,
                    "delta_24h_pp":        delta_24h,
                    "last_signal_score":   round(last_signal_score, 2) if last_signal_score is not None else None,
                    "last_signal_types":   last_signal_types,
                    "last_signal_at":      last_signal_at,
                    "hours_since_signal":  round(hours_since_signal, 2) if hours_since_signal is not None else None,
                    "decision_priority":   hotness,
                })

            enriched_items.sort(
                key=lambda it: (float(it.get("decision_priority") or 0.0), it.get("added_at") or ""),
                reverse=True,
            )

            item_count   = len(enriched_items)
            avg_priority = round(hotness_total / item_count, 2) if item_count else 0.0
            out.append({
                "id":                wl["id"],
                "name":              wl["name"],
                "created_at":        wl["created_at"],
                "updated_at":        wl["updated_at"],
                "item_count":        item_count,
                "active_signals_24h": active_signals_24h,
                "stale_items":       stale_items,
                "avg_priority":      avg_priority,
                "items":             enriched_items,
            })

        return out

    def remove_watchlist_item(self, item_id: int) -> bool:
        """Remove watchlist item by id."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT watchlist_id FROM watchlist_items WHERE id = ?",
                (item_id,),
            ).fetchone()
            rows = conn.execute(
                "DELETE FROM watchlist_items WHERE id = ?",
                (item_id,),
            ).rowcount
            if rows and row:
                conn.execute(
                    "UPDATE watchlists SET updated_at = ? WHERE id = ?",
                    (_utcnow().isoformat(), int(row["watchlist_id"])),
                )
        return rows > 0

    # ==================== Thesis Threads ====================

    def follow_thesis(
        self,
        thesis_key: str,
        title: str,
        category: str,
        note: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Create/update a followed thesis and append a follow event."""
        now = _utcnow().isoformat()
        seed_text = ((payload or {}).get("market_name") or title or "").strip()
        topic_terms = self._topic_terms(seed_text)
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO thesis_threads (thesis_key, title, category, topic_terms, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(thesis_key) DO UPDATE SET
                    title = excluded.title,
                    category = excluded.category,
                    topic_terms = CASE
                        WHEN excluded.topic_terms IS NOT NULL AND excluded.topic_terms != '[]'
                        THEN excluded.topic_terms
                        ELSE thesis_threads.topic_terms
                    END,
                    updated_at = excluded.updated_at
                """,
                (thesis_key, title, category, json.dumps(topic_terms), now, now),
            )
            row = conn.execute(
                "SELECT id FROM thesis_threads WHERE thesis_key = ?",
                (thesis_key,),
            ).fetchone()
            thread_id = int(row["id"])
            conn.execute(
                """
                INSERT INTO thesis_updates (thread_id, event_type, note, payload_json, created_at)
                VALUES (?, 'follow', ?, ?, ?)
                """,
                (thread_id, note, json.dumps(payload or {}), now),
            )
        return thread_id

    def add_thesis_note(
        self,
        thesis_key: str,
        note: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Append a note update to an existing thesis."""
        now = _utcnow().isoformat()
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT id FROM thesis_threads WHERE thesis_key = ?",
                (thesis_key,),
            ).fetchone()
            if not row:
                return False
            thread_id = int(row["id"])
            conn.execute(
                """
                INSERT INTO thesis_updates (thread_id, event_type, note, payload_json, created_at)
                VALUES (?, 'note', ?, ?, ?)
                """,
                (thread_id, note, json.dumps(payload or {}), now),
            )
            conn.execute(
                "UPDATE thesis_threads SET updated_at = ? WHERE id = ?",
                (now, thread_id),
            )
        return True

    def add_thesis_action(
        self,
        thesis_key: str,
        action: str,
        rationale: str = "",
        payload: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Append an action event to an existing thesis thread."""
        action = (action or "").strip()
        if not action:
            return False

        note = f"Action queued: {action}"
        if rationale:
            note += f" — {rationale.strip()}"

        now = _utcnow().isoformat()
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT id FROM thesis_threads WHERE thesis_key = ?",
                (thesis_key,),
            ).fetchone()
            if not row:
                return False
            thread_id = int(row["id"])
            conn.execute(
                """
                INSERT INTO thesis_updates (thread_id, event_type, note, payload_json, created_at)
                VALUES (?, 'action', ?, ?, ?)
                """,
                (thread_id, note, json.dumps(payload or {}), now),
            )
            conn.execute(
                "UPDATE thesis_threads SET updated_at = ? WHERE id = ?",
                (now, thread_id),
            )
        return True

    def get_thesis_threads(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Return followed thesis threads with recent updates."""
        with self._get_conn() as conn:
            threads = conn.execute(
                """
                SELECT id, thesis_key, title, category, status, created_at, updated_at
                FROM thesis_threads
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            result: List[Dict[str, Any]] = []
            for t in threads:
                updates = conn.execute(
                    """
                    SELECT event_type, note, payload_json, created_at
                    FROM thesis_updates
                    WHERE thread_id = ?
                    ORDER BY created_at DESC
                    LIMIT 8
                    """,
                    (t["id"],),
                ).fetchall()
                parsed_updates = []
                for u in updates:
                    try:
                        payload = json.loads(u["payload_json"] or "{}")
                    except json.JSONDecodeError:
                        payload = {}
                    parsed_updates.append({
                        "event_type": u["event_type"],
                        "note": u["note"],
                        "payload": payload,
                        "created_at": u["created_at"],
                    })
                result.append({
                    "thesis_key": t["thesis_key"],
                    "title": t["title"],
                    "category": t["category"],
                    "topic_terms": json.loads(t["topic_terms"] or "[]") if "topic_terms" in t.keys() else [],
                    "status": t["status"],
                    "created_at": t["created_at"],
                    "updated_at": t["updated_at"],
                    "updates": parsed_updates,
                })
        return result

    @staticmethod
    def _parse_iso(ts: Any) -> Optional[datetime]:
        if not isinstance(ts, str):
            return None
        try:
            return datetime.fromisoformat(ts)
        except ValueError:
            return None

    def _build_thesis_catalysts(
        self,
        text_blob: str,
        signal_counts: Dict[str, int],
        recency_hours: Optional[float],
        max_items: int = 4,
    ) -> List[Dict[str, Any]]:
        blob = (text_blob or "").lower()
        catalysts: List[Dict[str, Any]] = []
        seen = set()

        def _add(key: str, title: str, why: str, urgency: int):
            if key in seen:
                return
            seen.add(key)
            catalysts.append({
                "key": key,
                "title": title,
                "why": why,
                "urgency": int(self._clamp(float(urgency), 1, 100)),
            })

        keyword_map = [
            (("cpi", "inflation", "core pce"), "macro-inflation", "Inflation print / macro release", "Inflation surprises can reprice rates and policy odds quickly.", 78),
            (("fed", "fomc", "rate cut", "interest rate", "powell"), "macro-rates", "Fed communication and rates path", "Policy-path messaging is a high-conviction driver for macro-linked markets.", 82),
            (("jobs", "payroll", "nfp", "unemployment"), "macro-labor", "Labor-market data shock", "Labor data can abruptly shift recession and policy expectations.", 72),
            (("earnings", "guidance", "revenue", "eps"), "corp-earnings", "Earnings/guidance catalyst", "Forward guidance can flip sentiment faster than trailing results.", 68),
            (("debate", "primary", "election", "senate", "congress"), "political-calendar", "Political calendar event", "Debates, primaries, and vote deadlines drive binary repricing.", 74),
            (("ceasefire", "troops", "missile", "attack", "invasion"), "geo-escalation", "Geopolitical escalation/de-escalation", "Conflict headlines create step-function repricing risk.", 80),
            (("court", "lawsuit", "sec", "ruling", "supreme"), "legal-ruling", "Regulatory or legal ruling", "Legal outcomes can invalidate one scenario immediately.", 70),
            (("bitcoin", "eth", "crypto", "etf"), "crypto-flow", "Crypto flow/regulatory catalyst", "ETF/regulatory flow often drives fast, reflexive positioning.", 66),
            (("ai", "openai", "anthropic", "chip", "semiconductor", "nvidia"), "ai-cycle", "AI/semiconductor cycle update", "AI demand/supply updates can re-anchor growth assumptions.", 64),
            (("tariff", "sanction", "trade war"), "policy-shock", "Policy shock / trade action", "Policy shocks can propagate through correlated markets quickly.", 69),
        ]
        for patterns, key, title, why, urgency in keyword_map:
            if any(p in blob for p in patterns):
                _add(key, title, why, urgency)

        if signal_counts.get("whale_activity", 0) > 0:
            _add(
                "whale-follow-through",
                "Whale flow follow-through",
                "Smart-money participation suggests potential information edge; confirm whether flow persists.",
                77,
            )
        if signal_counts.get("cross_market_divergence", 0) > 0:
            _add(
                "cross-venue-convergence",
                "Cross-venue convergence",
                "Divergence often resolves quickly; monitor whether lagging markets catch up or leader mean-reverts.",
                75,
            )
        if signal_counts.get("orderbook_imbalance", 0) > 0 or signal_counts.get("thin_liquidity_jump", 0) > 0:
            _add(
                "liquidity-regime",
                "Liquidity regime shift",
                "Book imbalance/thin liquidity can amplify small news into outsized moves.",
                69,
            )
        if signal_counts.get("no_news_move", 0) > 0:
            _add(
                "news-confirmation-gap",
                "News confirmation gap",
                "Move without broad coverage should either be confirmed by later reporting or mean-revert.",
                74,
            )

        if not catalysts:
            _add(
                "flow-continuation",
                "Flow continuation check",
                "Track whether repeated signal clusters confirm this thesis beyond a single repricing event.",
                62,
            )

        if recency_hours is not None and recency_hours <= 2:
            for c in catalysts:
                c["urgency"] = int(self._clamp(c["urgency"] + 8, 1, 100))

        catalysts.sort(key=lambda c: c["urgency"], reverse=True)
        return catalysts[:max_items]

    def _build_thesis_falsifiers(
        self,
        current_prob: float,
        net_direction: float,
        avg_signal_score: float,
    ) -> List[Dict[str, str]]:
        base = float(self._clamp(current_prob, 1.0, 99.0))
        directional_buffer = 6.0 if avg_signal_score >= 60 else 4.0
        invalidation_level = round(
            base - directional_buffer if net_direction >= 0 else base + directional_buffer,
            1,
        )
        drift_guard = round(
            base + directional_buffer if net_direction >= 0 else base - directional_buffer,
            1,
        )

        return [
            {
                "condition": f"Probability closes beyond {invalidation_level:.1f}% against thesis direction for 2 consecutive snapshots.",
                "why": "Sustained adverse repricing indicates the market rejected the thesis, not just noise.",
            },
            {
                "condition": "Median related signal score falls below 45 across the next 3 related alerts.",
                "why": "Signal quality deterioration suggests the original edge has decayed.",
            },
            {
                "condition": f"Price fails to hold above/below {drift_guard:.1f}% after top catalyst window.",
                "why": "Failed post-catalyst follow-through is a classic invalidation pattern.",
            },
        ]

    def _build_thesis_scenario_tree(
        self,
        current_prob: float,
        net_direction: float,
    ) -> List[Dict[str, Any]]:
        p = float(self._clamp(current_prob, 1.0, 99.0))
        bullish = net_direction >= 0
        if bullish:
            confirm_low = self._clamp(p + 6, 1, 99)
            confirm_high = self._clamp(p + 16, 1, 99)
            base_low = self._clamp(p - 4, 1, 99)
            base_high = self._clamp(p + 6, 1, 99)
            fail_low = self._clamp(p - 18, 1, 99)
            fail_high = self._clamp(p - 6, 1, 99)
            confirm_trigger = "Catalyst confirms + signal score remains ≥ 60."
            fail_trigger = "Catalyst disappoints or adverse headline arrives."
        else:
            confirm_low = self._clamp(p - 16, 1, 99)
            confirm_high = self._clamp(p - 6, 1, 99)
            base_low = self._clamp(p - 6, 1, 99)
            base_high = self._clamp(p + 4, 1, 99)
            fail_low = self._clamp(p + 6, 1, 99)
            fail_high = self._clamp(p + 18, 1, 99)
            confirm_trigger = "Catalyst confirms downside + signal score remains ≥ 60."
            fail_trigger = "Catalyst surprises positive or flow reverses."

        return [
            {
                "scenario": "Confirm",
                "trigger": confirm_trigger,
                "probability_range": [round(confirm_low, 1), round(confirm_high, 1)],
                "implication": "Lean with thesis and prioritize execution speed.",
            },
            {
                "scenario": "Base",
                "trigger": "Mixed evidence; no strong catalyst surprise.",
                "probability_range": [round(base_low, 1), round(base_high, 1)],
                "implication": "Hold optionality and wait for cleaner confirmation.",
            },
            {
                "scenario": "Invalidate",
                "trigger": fail_trigger,
                "probability_range": [round(fail_low, 1), round(fail_high, 1)],
                "implication": "De-risk thesis and rotate to better-asymmetric setup.",
            },
        ]

    def _build_thesis_actions(
        self,
        catalysts: List[Dict[str, Any]],
        falsifiers: List[Dict[str, str]],
        decision_sla_minutes: int,
        current_prob: float,
        net_direction: float,
    ) -> List[Dict[str, Any]]:
        p = float(self._clamp(current_prob, 1.0, 99.0))
        invalidation = round(p - 5.5 if net_direction >= 0 else p + 5.5, 1)
        top_catalyst = catalysts[0]["title"] if catalysts else "next catalyst window"
        top_falsifier = falsifiers[0]["condition"] if falsifiers else "thesis invalidation condition"

        return [
            {
                "priority": 1,
                "action": f"Set explicit invalidation guardrail at {invalidation:.1f}%.",
                "why": "Pre-committed exits reduce decision drift during volatility.",
                "eta_minutes": 10,
            },
            {
                "priority": 2,
                "action": f"Monitor catalyst: {top_catalyst}.",
                "why": "Catalyst timing determines whether this thesis confirms or fades.",
                "eta_minutes": max(15, min(decision_sla_minutes, 120)),
            },
            {
                "priority": 3,
                "action": "Review closest historical analog outcomes before sizing conviction.",
                "why": "Analog hit-rate context prevents overreaction to a single move.",
                "eta_minutes": 20,
            },
            {
                "priority": 4,
                "action": f"Run falsification check: {top_falsifier}",
                "why": "Fast invalidation checks preserve capital and attention.",
                "eta_minutes": decision_sla_minutes,
            },
        ]

    def get_thesis_copilot_threads(
        self,
        limit: int = 20,
        alert_lookback_days: int = 21,
    ) -> List[Dict[str, Any]]:
        """
        Return thesis threads enriched with copilot workflow fields:
        catalysts, falsifiers, scenario tree, urgency/SLA, and next actions.
        """
        now = _utcnow()
        cutoff = (now - timedelta(days=alert_lookback_days)).isoformat()

        with self._get_conn() as conn:
            threads = conn.execute(
                """
                SELECT id, thesis_key, title, category, status, topic_terms, created_at, updated_at
                FROM thesis_threads
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            watchlist_links_rows = conn.execute(
                """
                SELECT wi.platform, wi.market_id, wi.market_name, w.name AS watchlist_name
                FROM watchlist_items wi
                JOIN watchlists w ON w.id = wi.watchlist_id
                """
            ).fetchall()
            watchlists_by_key: Dict[str, set] = {}
            watchlists_by_name: Dict[str, set] = {}
            for r in watchlist_links_rows:
                key = f"{(r['platform'] or '').lower()}:{r['market_id']}"
                watchlists_by_key.setdefault(key, set()).add(r["watchlist_name"])
                nm = (r["market_name"] or "").strip().lower()
                if nm:
                    watchlists_by_name.setdefault(nm, set()).add(r["watchlist_name"])

            alert_cache: Dict[str, List[sqlite3.Row]] = {}

            def _alerts_for_category(category_norm: str) -> List[sqlite3.Row]:
                ck = category_norm.lower() if category_norm else "__all__"
                if ck in alert_cache:
                    return alert_cache[ck]
                if category_norm and category_norm != "OTHER":
                    rows = conn.execute(
                        """
                        SELECT market_name, platform, market_id, signal_score, signal_types,
                               old_probability, new_probability, timestamp, outcome_label,
                               outcome_magnitude, time_to_hit_minutes, market_category
                        FROM alert_history
                        WHERE timestamp > ?
                          AND (LOWER(market_category) = ? OR market_category IS NULL OR market_category = '')
                        ORDER BY timestamp DESC
                        LIMIT 1500
                        """,
                        (cutoff, category_norm.lower()),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT market_name, platform, market_id, signal_score, signal_types,
                               old_probability, new_probability, timestamp, outcome_label,
                               outcome_magnitude, time_to_hit_minutes, market_category
                        FROM alert_history
                        WHERE timestamp > ?
                        ORDER BY timestamp DESC
                        LIMIT 1500
                        """,
                        (cutoff,),
                    ).fetchall()
                alert_cache[ck] = list(rows)
                return alert_cache[ck]

            result: List[Dict[str, Any]] = []
            for thread in threads:
                updates_rows = conn.execute(
                    """
                    SELECT event_type, note, payload_json, created_at
                    FROM thesis_updates
                    WHERE thread_id = ?
                    ORDER BY created_at DESC
                    LIMIT 12
                    """,
                    (thread["id"],),
                ).fetchall()
                updates = []
                for row in updates_rows:
                    updates.append({
                        "event_type": row["event_type"],
                        "note": row["note"],
                        "payload": self._safe_json_dict(row["payload_json"]),
                        "created_at": row["created_at"],
                    })

                topic_terms = [str(t) for t in self._safe_json_list(thread["topic_terms"])]
                topic_set = set(topic_terms)
                topic_set.update(self._topic_terms(thread["title"] or ""))
                for up in updates[:5]:
                    mkt = str((up.get("payload") or {}).get("market_name") or "")
                    topic_set.update(self._topic_terms(mkt))
                topic_set = {t for t in topic_set if t}

                category_norm = (thread["category"] or "OTHER").strip().upper() or "OTHER"
                candidates = _alerts_for_category(category_norm)
                related = []
                title_l = (thread["title"] or "").strip().lower()
                for alert in candidates:
                    market_name = alert["market_name"] or ""
                    alert_terms = set(self._topic_terms(market_name))
                    title_hit = bool(title_l and title_l[:42] in market_name.lower())
                    overlap = 0.0
                    if topic_set and alert_terms:
                        overlap = len(topic_set & alert_terms) / max(1, min(len(topic_set), len(alert_terms)))
                    if overlap < 0.22 and not title_hit:
                        continue
                    row = dict(alert)
                    row["similarity"] = round(max(overlap, 0.68 if title_hit else overlap), 4)
                    row["signal_types"] = [str(s) for s in self._safe_json_list(row.get("signal_types"))]
                    related.append(row)

                related.sort(
                    key=lambda r: (
                        float(r.get("similarity") or 0.0),
                        float(r.get("signal_score") or 0.0),
                        str(r.get("timestamp") or ""),
                    ),
                    reverse=True,
                )
                related = related[:30]

                recency_hours: Optional[float] = None
                max_abs_change = 0.0
                signal_scores: List[float] = []
                deltas: List[float] = []
                signal_counts: Dict[str, int] = {}
                related_watchlists = set()
                related_markets = set()
                recent_24h = 0
                newest_ts = None

                for row in related:
                    ts = self._parse_iso(row.get("timestamp"))
                    if ts:
                        newest_ts = ts if newest_ts is None or ts > newest_ts else newest_ts
                        age_h = max(0.0, (now - ts).total_seconds() / 3600.0)
                        if age_h <= 24:
                            recent_24h += 1

                    old_p = row.get("old_probability")
                    new_p = row.get("new_probability")
                    if old_p is not None and new_p is not None:
                        delta = float(new_p) - float(old_p)
                        deltas.append(delta)
                        max_abs_change = max(max_abs_change, abs(delta))
                    if row.get("signal_score") is not None:
                        signal_scores.append(float(row["signal_score"]))
                    for st in row.get("signal_types", []):
                        signal_counts[st] = signal_counts.get(st, 0) + 1

                    mk = f"{(row.get('platform') or '').lower()}:{row.get('market_id')}"
                    related_markets.add(mk)
                    if mk in watchlists_by_key:
                        related_watchlists.update(watchlists_by_key[mk])
                    nm_l = str(row.get("market_name") or "").strip().lower()
                    if nm_l in watchlists_by_name:
                        related_watchlists.update(watchlists_by_name[nm_l])

                if newest_ts:
                    recency_hours = max(0.0, (now - newest_ts).total_seconds() / 3600.0)

                avg_signal = (sum(signal_scores) / len(signal_scores)) if signal_scores else 0.0
                net_direction = sum(deltas) if deltas else 0.0

                current_prob = 50.0
                latest_related = related[0] if related else None
                if latest_related and latest_related.get("new_probability") is not None:
                    current_prob = float(latest_related["new_probability"])
                else:
                    anchor_market_id = None
                    anchor_platform = None
                    for up in updates:
                        payload = up.get("payload") or {}
                        if payload.get("market_id"):
                            anchor_market_id = str(payload.get("market_id"))
                            anchor_platform = str(payload.get("platform") or "").lower()
                            break
                    if anchor_market_id and anchor_platform:
                        snap = conn.execute(
                            """
                            SELECT probability
                            FROM market_snapshots
                            WHERE platform = ? AND market_id = ?
                            ORDER BY timestamp DESC
                            LIMIT 1
                            """,
                            (anchor_platform, anchor_market_id),
                        ).fetchone()
                        if snap:
                            current_prob = float(snap["probability"])

                recency_bonus = 0.0
                if recency_hours is not None:
                    if recency_hours <= 2:
                        recency_bonus = 24.0
                    elif recency_hours <= 6:
                        recency_bonus = 16.0
                    elif recency_hours <= 24:
                        recency_bonus = 8.0
                urgency_score = self._clamp(
                    avg_signal * 0.55 + max_abs_change * 4.5 + min(16.0, recent_24h * 2.0) + recency_bonus,
                    0.0,
                    100.0,
                )
                if urgency_score >= 78:
                    decision_sla_minutes = 30
                elif urgency_score >= 62:
                    decision_sla_minutes = 90
                elif urgency_score >= 45:
                    decision_sla_minutes = 240
                else:
                    decision_sla_minutes = 720

                text_blob_parts = [thread["title"] or ""]
                text_blob_parts.extend([u.get("note") or "" for u in updates[:8]])
                text_blob_parts.extend([r.get("market_name") or "" for r in related[:10]])
                text_blob = " ".join(text_blob_parts)
                catalysts = self._build_thesis_catalysts(
                    text_blob=text_blob,
                    signal_counts=signal_counts,
                    recency_hours=recency_hours,
                    max_items=4,
                )
                falsifiers = self._build_thesis_falsifiers(
                    current_prob=current_prob,
                    net_direction=net_direction,
                    avg_signal_score=avg_signal,
                )
                scenario_tree = self._build_thesis_scenario_tree(
                    current_prob=current_prob,
                    net_direction=net_direction,
                )
                next_actions = self._build_thesis_actions(
                    catalysts=catalysts,
                    falsifiers=falsifiers,
                    decision_sla_minutes=decision_sla_minutes,
                    current_prob=current_prob,
                    net_direction=net_direction,
                )

                summary = (
                    f"{recent_24h} related alert(s) in 24h across {len(related_markets)} market(s); "
                    f"avg signal {avg_signal:.1f}, max move {max_abs_change:.1f}pp."
                )

                result.append({
                    "thesis_key": thread["thesis_key"],
                    "title": thread["title"],
                    "category": thread["category"],
                    "status": thread["status"],
                    "topic_terms": topic_terms,
                    "created_at": thread["created_at"],
                    "updated_at": thread["updated_at"],
                    "updates": updates,
                    "copilot": {
                        "summary": summary,
                        "current_probability": round(current_prob, 2),
                        "urgency_score": round(float(urgency_score), 2),
                        "decision_sla_minutes": int(decision_sla_minutes),
                        "recent_related_alerts_24h": recent_24h,
                        "related_markets_count": len(related_markets),
                        "linked_watchlists": sorted(related_watchlists),
                        "linked_watchlists_count": len(related_watchlists),
                        "signal_type_counts": signal_counts,
                        "catalysts": catalysts,
                        "falsifiers": falsifiers,
                        "scenario_tree": scenario_tree,
                        "next_best_actions": next_actions,
                        "top_related_alerts": related[:5],
                    },
                })

        return result

    def link_alert_to_followed_thesis(
        self,
        market_name: str,
        category: str,
        platform: str,
        market_id: str,
        signal_score: float,
        signal_types: Optional[List[str]] = None,
    ) -> Optional[str]:
        """
        Auto-attach an incoming alert to the best matching active thesis thread.
        Returns matched thesis_key or None if no sufficiently similar thread exists.
        """
        alert_terms = set(self._topic_terms(market_name))
        if not alert_terms:
            return None

        category_norm = (category or "").strip().upper()
        now = _utcnow().isoformat()

        with self._get_conn() as conn:
            if category_norm:
                threads = conn.execute(
                    """
                    SELECT id, thesis_key, title, category, topic_terms
                    FROM thesis_threads
                    WHERE status = 'active'
                      AND (category = ? OR category IS NULL OR category = '')
                    ORDER BY updated_at DESC
                    LIMIT 100
                    """,
                    (category_norm,),
                ).fetchall()
            else:
                threads = conn.execute(
                    """
                    SELECT id, thesis_key, title, category, topic_terms
                    FROM thesis_threads
                    WHERE status = 'active'
                    ORDER BY updated_at DESC
                    LIMIT 100
                    """
                ).fetchall()

            best = None
            best_score = 0.0
            for thread in threads:
                try:
                    thread_terms = set(json.loads(thread["topic_terms"] or "[]"))
                except json.JSONDecodeError:
                    thread_terms = set()
                if not thread_terms:
                    thread_terms = set(self._topic_terms(thread["title"] or ""))
                if not thread_terms:
                    continue

                overlap = len(alert_terms & thread_terms) / max(1, min(len(alert_terms), len(thread_terms)))
                title_hit = (thread["title"] or "").lower() in (market_name or "").lower()
                if title_hit:
                    overlap = max(overlap, 0.7)

                if overlap > best_score:
                    best_score = overlap
                    best = thread

            if not best or best_score < 0.42:
                return None

            note = (
                f"Auto-update: new {platform.title()} signal "
                f"({signal_score:.0f}/100) on “{market_name}”."
            )
            conn.execute(
                """
                INSERT INTO thesis_updates (thread_id, event_type, note, payload_json, created_at)
                VALUES (?, 'signal', ?, ?, ?)
                """,
                (
                    best["id"],
                    note,
                    json.dumps({
                        "market_name": market_name,
                        "market_id": market_id,
                        "platform": platform,
                        "signal_score": signal_score,
                        "signal_types": signal_types or [],
                        "similarity": round(best_score, 4),
                    }),
                    now,
                ),
            )
            conn.execute(
                "UPDATE thesis_threads SET updated_at = ? WHERE id = ?",
                (now, best["id"]),
            )
            return best["thesis_key"]

    # ==================== Decision Workflow ====================

    def get_recent_alert_candidates(
        self,
        category: Optional[str] = None,
        days: int = 120,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Candidate alerts for analog search and calibration UI."""
        cutoff = (_utcnow() - timedelta(days=days)).isoformat()
        with self._get_conn() as conn:
            if category:
                rows = conn.execute(
                    """
                    SELECT id, market_name, platform, signal_score, reasons, signal_types, market_category,
                           old_probability, new_probability, outcome_label, outcome_magnitude,
                           time_to_hit_minutes, timestamp
                    FROM alert_history
                    WHERE timestamp > ? AND market_category = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (cutoff, category, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, market_name, platform, signal_score, reasons, signal_types, market_category,
                           old_probability, new_probability, outcome_label, outcome_magnitude,
                           time_to_hit_minutes, timestamp
                    FROM alert_history
                    WHERE timestamp > ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (cutoff, limit),
                ).fetchall()
        return [dict(r) for r in rows]

    # ==================== Order Book Snapshots ====================

    def save_orderbook_snapshot(
        self,
        platform: str,
        market_id: str,
        bid_depth: float,
        ask_depth: float,
        spread: float,
        best_bid: float,
        best_ask: float,
        bid_ask_ratio: float,
        top_levels: Optional[Dict] = None,
    ):
        """Save an order book snapshot."""
        timestamp = _utcnow().isoformat()

        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO orderbook_snapshots
                (platform, market_id, bid_depth, ask_depth, spread,
                 best_bid, best_ask, bid_ask_ratio, top_levels, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                platform, market_id, bid_depth, ask_depth, spread,
                best_bid, best_ask, bid_ask_ratio,
                json.dumps(top_levels) if top_levels else None,
                timestamp,
            ))

    def get_recent_orderbook_snapshots(
        self,
        platform: str,
        market_id: str,
        minutes: int = 60,
    ) -> List[Dict[str, Any]]:
        """Get order book snapshots from the last N minutes."""
        cutoff = (_utcnow() - timedelta(minutes=minutes)).isoformat()

        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM orderbook_snapshots
                WHERE platform = ? AND market_id = ? AND timestamp > ?
                ORDER BY timestamp ASC
            """, (platform, market_id, cutoff)).fetchall()

        return [dict(row) for row in rows]

    def get_latest_orderbook(
        self,
        platform: str,
        market_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent order book snapshot."""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT * FROM orderbook_snapshots
                WHERE platform = ? AND market_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (platform, market_id)).fetchone()

        return dict(row) if row else None

    def get_orderbook_baseline(
        self,
        platform: str,
        market_id: str,
        hours: int = 24,
    ) -> Optional[Dict[str, float]]:
        """Get average order book metrics over baseline period."""
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()

        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT AVG(bid_depth) as avg_bid_depth,
                       AVG(ask_depth) as avg_ask_depth,
                       AVG(spread) as avg_spread,
                       AVG(bid_ask_ratio) as avg_ratio,
                       COUNT(*) as cnt
                FROM orderbook_snapshots
                WHERE platform = ? AND market_id = ? AND timestamp > ?
            """, (platform, market_id, cutoff)).fetchone()

        if not row or row["cnt"] == 0:
            return None

        return {
            "avg_bid_depth": row["avg_bid_depth"],
            "avg_ask_depth": row["avg_ask_depth"],
            "avg_spread": row["avg_spread"],
            "avg_ratio": row["avg_ratio"],
            "sample_count": row["cnt"],
        }

    # ==================== Hourly Volume Baselines ====================

    def update_hourly_volume_baseline(
        self,
        platform: str,
        market_id: str,
        hour_utc: int,
        volume: float,
    ):
        """
        Update rolling average volume for a given hour of day.
        Uses exponential moving average to weight recent data more.
        """
        timestamp = _utcnow().isoformat()

        with self._get_conn() as conn:
            existing = conn.execute("""
                SELECT avg_volume, sample_count FROM hourly_volume_baselines
                WHERE platform = ? AND market_id = ? AND hour_utc = ?
            """, (platform, market_id, hour_utc)).fetchone()

            if existing:
                # Exponential moving average: weight recent samples more
                old_avg = existing["avg_volume"]
                count = existing["sample_count"]
                # EMA with alpha that decreases as we get more samples (min alpha=0.1)
                alpha = max(0.1, 1.0 / (count + 1))
                new_avg = old_avg * (1 - alpha) + volume * alpha
                new_count = count + 1

                conn.execute("""
                    UPDATE hourly_volume_baselines
                    SET avg_volume = ?, sample_count = ?, last_updated = ?
                    WHERE platform = ? AND market_id = ? AND hour_utc = ?
                """, (new_avg, new_count, timestamp, platform, market_id, hour_utc))
            else:
                conn.execute("""
                    INSERT INTO hourly_volume_baselines
                    (platform, market_id, hour_utc, avg_volume, sample_count, last_updated)
                    VALUES (?, ?, ?, ?, 1, ?)
                """, (platform, market_id, hour_utc, volume, timestamp))

    def get_hourly_volume_baseline(
        self,
        platform: str,
        market_id: str,
        hour_utc: int,
    ) -> Optional[Dict[str, Any]]:
        """Get baseline volume for a specific hour of day."""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT avg_volume, sample_count FROM hourly_volume_baselines
                WHERE platform = ? AND market_id = ? AND hour_utc = ?
            """, (platform, market_id, hour_utc)).fetchone()

        if row:
            return {"avg_volume": row["avg_volume"], "sample_count": row["sample_count"]}
        return None

    def get_all_hourly_baselines(
        self,
        platform: str,
        market_id: str,
    ) -> Dict[int, float]:
        """Get all 24 hourly baselines for a market. Returns {hour: avg_volume}."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT hour_utc, avg_volume FROM hourly_volume_baselines
                WHERE platform = ? AND market_id = ?
            """, (platform, market_id)).fetchall()

        return {row["hour_utc"]: row["avg_volume"] for row in rows}

    # ==================== Whale Tracking ====================

    def upsert_whale_wallet(
        self,
        address: str,
        total_trades: int = 0,
        winning_trades: int = 0,
        total_volume: float = 0.0,
        label: Optional[str] = None,
        is_whale: bool = False,
    ):
        """Insert or update a whale wallet record."""
        now = _utcnow().isoformat()
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0

        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO whale_wallets
                (address, label, total_trades, winning_trades, win_rate,
                 total_volume, first_seen, last_seen, is_whale)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(address) DO UPDATE SET
                    total_trades = ?,
                    winning_trades = ?,
                    win_rate = ?,
                    total_volume = ?,
                    last_seen = ?,
                    is_whale = ?,
                    label = COALESCE(?, whale_wallets.label)
            """, (
                address, label, total_trades, winning_trades, win_rate,
                total_volume, now, now, int(is_whale),
                total_trades, winning_trades, win_rate,
                total_volume, now, int(is_whale), label,
            ))

    def save_whale_trade(
        self,
        address: str,
        market_id: str,
        direction: str,
        amount: float,
        price: float,
        tx_hash: str,
        market_name: Optional[str] = None,
    ):
        """Save a whale trade event."""
        timestamp = _utcnow().isoformat()

        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO whale_trades
                (address, market_id, market_name, direction, amount, price, tx_hash, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (address, market_id, market_name, direction, amount, price, tx_hash, timestamp))

    def get_recent_whale_trades(
        self,
        market_id: Optional[str] = None,
        minutes: int = 60,
    ) -> List[Dict[str, Any]]:
        """Get recent whale trades for a market."""
        cutoff = (_utcnow() - timedelta(minutes=minutes)).isoformat()

        with self._get_conn() as conn:
            if market_id is None:
                rows = conn.execute("""
                    SELECT wt.*, ww.win_rate, ww.total_volume as wallet_volume,
                           ww.total_trades as wallet_trades, ww.is_whale
                    FROM whale_trades wt
                    LEFT JOIN whale_wallets ww ON wt.address = ww.address
                    WHERE wt.timestamp > ?
                    ORDER BY wt.timestamp DESC
                """, (cutoff,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT wt.*, ww.win_rate, ww.total_volume as wallet_volume,
                           ww.total_trades as wallet_trades, ww.is_whale
                    FROM whale_trades wt
                    LEFT JOIN whale_wallets ww ON wt.address = ww.address
                    WHERE wt.market_id = ? AND wt.timestamp > ?
                    ORDER BY wt.timestamp DESC
                """, (market_id, cutoff)).fetchall()

        return [dict(row) for row in rows]

    def get_whale_wallets(
        self,
        min_volume: float = 0,
        min_win_rate: float = 0,
        only_whales: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get whale wallets matching criteria."""
        with self._get_conn() as conn:
            query = """
                SELECT * FROM whale_wallets
                WHERE total_volume >= ? AND win_rate >= ?
            """
            params: list = [min_volume, min_win_rate]

            if only_whales:
                query += " AND is_whale = 1"

            query += " ORDER BY total_volume DESC"

            rows = conn.execute(query, params).fetchall()

        return [dict(row) for row in rows]

    # ==================== News Cache ====================

    def save_news_article(
        self,
        title: str,
        source: str,
        url: str,
        published_at: Optional[str] = None,
        keywords: Optional[List[str]] = None,
    ):
        """Cache a news article."""
        fetched_at = _utcnow().isoformat()

        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO news_cache
                (title, source, url, published_at, keywords, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                title, source, url, published_at,
                json.dumps(keywords) if keywords else None,
                fetched_at,
            ))

    def search_recent_news(
        self,
        search_terms: List[str],
        hours: int = 4,
    ) -> List[Dict[str, Any]]:
        """
        Search news cache for articles matching any of the search terms.
        Returns articles from the last N hours.
        """
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()

        with self._get_conn() as conn:
            # Build LIKE clauses for each search term
            conditions = []
            params: list = []
            for term in search_terms:
                conditions.append("(LOWER(title) LIKE ? OR LOWER(keywords) LIKE ?)")
                term_pattern = f"%{term.lower()}%"
                params.extend([term_pattern, term_pattern])

            if not conditions:
                return []

            where_clause = " OR ".join(conditions)
            params.append(cutoff)

            rows = conn.execute(f"""
                SELECT * FROM news_cache
                WHERE ({where_clause}) AND fetched_at > ?
                ORDER BY fetched_at DESC
            """, params).fetchall()

        return [dict(row) for row in rows]

    def get_all_recent_news(self, hours: int = 24, limit: int = 20) -> List[Dict[str, Any]]:
        """Return the most recent N news articles regardless of content."""
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT title, source, url, published_at, fetched_at
                FROM news_cache
                WHERE fetched_at > ?
                ORDER BY fetched_at DESC
                LIMIT ?
            """, (cutoff, limit)).fetchall()
        return [dict(row) for row in rows]

    def count_recent_news(self, hours: int = 4) -> int:
        """Count total news articles in the last N hours."""
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()

        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT COUNT(*) as count FROM news_cache
                WHERE fetched_at > ?
            """, (cutoff,)).fetchone()

        return row["count"] if row else 0

    # ==================== Dashboard Feed ====================

    def get_recent_alerts_feed(
        self,
        hours: int = 24,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get recent alerts for the dashboard story feed, newest first."""
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()

        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM alert_history
                WHERE timestamp > ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (cutoff, limit)).fetchall()

        return [dict(row) for row in rows]

    def get_recent_movers(
        self,
        hours: int = 2,
        min_change: float = 1.5,
        limit: int = 24,
    ) -> List[Dict[str, Any]]:
        """
        Find markets with notable price movement in the last N hours.
        Used for the dashboard radar section — markets approaching signal
        threshold but not yet confirmed as alerts.
        """
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()

        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT
                    m1.platform,
                    m1.market_id,
                    m1.market_name,
                    m1.probability      AS latest_prob,
                    m2.probability      AS oldest_prob,
                    m1.probability - m2.probability AS change,
                    ABS(m1.probability - m2.probability) AS abs_change,
                    m1.volume_24h,
                    m1.timestamp        AS latest_ts
                FROM market_snapshots m1
                JOIN market_snapshots m2
                    ON  m1.platform  = m2.platform
                    AND m1.market_id = m2.market_id
                WHERE m1.timestamp = (
                    SELECT MAX(ms.timestamp)
                    FROM market_snapshots ms
                    WHERE ms.platform  = m1.platform
                      AND ms.market_id = m1.market_id
                      AND ms.timestamp > ?
                )
                AND m2.timestamp = (
                    SELECT MIN(ms.timestamp)
                    FROM market_snapshots ms
                    WHERE ms.platform  = m2.platform
                      AND ms.market_id = m2.market_id
                      AND ms.timestamp > ?
                )
                AND ABS(m1.probability - m2.probability) >= ?
                GROUP BY m1.platform, m1.market_id
                ORDER BY abs_change DESC
                LIMIT ?
            """, (cutoff, cutoff, min_change, limit)).fetchall()

        return [dict(row) for row in rows]

    def get_resolved_context_markets(
        self,
        limit: int = 8,
        min_volume_24h: float = 200_000,
    ) -> List[Dict[str, Any]]:
        """
        Return recently settled markets (probability ≥97% or ≤3%) with high
        volume — these represent real-world events that have resolved.
        Used to populate the Resolved Context tab.
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT
                    platform,
                    market_id,
                    market_name,
                    probability,
                    volume_24h,
                    end_date,
                    timestamp AS latest_ts
                FROM market_snapshots
                WHERE timestamp = (
                    SELECT MAX(ms.timestamp)
                    FROM market_snapshots ms
                    WHERE ms.platform  = market_snapshots.platform
                      AND ms.market_id = market_snapshots.market_id
                )
                AND (probability >= 97.0 OR probability <= 3.0)
                AND volume_24h >= ?
                GROUP BY platform, market_id
                ORDER BY volume_24h DESC
                LIMIT ?
            """, (min_volume_24h, limit)).fetchall()

        return [dict(row) for row in rows]

    def get_top_volume_markets(
        self,
        limit: int = 30,
        hours: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Return the highest-volume markets from recent snapshots.
        Used to seed the radar with the most active markets even before
        price movement or signals have been detected.
        """
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()

        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT
                    platform,
                    market_id,
                    market_name,
                    probability AS latest_prob,
                    probability AS oldest_prob,
                    0.0         AS change,
                    0.0         AS abs_change,
                    volume_24h,
                    timestamp   AS latest_ts
                FROM market_snapshots
                WHERE timestamp = (
                    SELECT MAX(ms.timestamp)
                    FROM market_snapshots ms
                    WHERE ms.platform  = market_snapshots.platform
                      AND ms.market_id = market_snapshots.market_id
                      AND ms.timestamp > ?
                )
                AND volume_24h > 0
                GROUP BY platform, market_id
                ORDER BY volume_24h DESC
                LIMIT ?
            """, (cutoff, limit)).fetchall()

        return [dict(row) for row in rows]

    # ==================== Truth Engine: Move Events ====================

    def detect_market_move_events(
        self,
        window_minutes: int = 60,
        min_change_pp: float = 2.0,
        scan_minutes: int = 360,
        per_market_cooldown_minutes: int = 20,
        max_events: int = 5000,
    ) -> Dict[str, int]:
        """
        Detect notable price moves from raw snapshots, independent of alerts.
        This expands the truth set to all tracked market behavior.
        """
        now = _utcnow()
        last_scan_raw = self.get_state("truth_engine_last_move_scan", default=None)
        if isinstance(last_scan_raw, str):
            try:
                last_scan = datetime.fromisoformat(last_scan_raw)
            except ValueError:
                last_scan = now - timedelta(minutes=scan_minutes)
            cutoff_dt = last_scan - timedelta(minutes=window_minutes)
        else:
            cutoff_dt = now - timedelta(minutes=scan_minutes)
        cutoff = cutoff_dt.isoformat()

        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT platform, market_id, market_name, probability, volume_24h, timestamp
                FROM market_snapshots
                WHERE timestamp > ?
                ORDER BY platform, market_id, timestamp ASC
                """,
                (cutoff,),
            ).fetchall()

            # Build per-market category cache from alert_history.
            category_rows = conn.execute(
                """
                SELECT ah.platform, ah.market_id, ah.market_category
                FROM alert_history ah
                JOIN (
                    SELECT platform, market_id, MAX(timestamp) AS max_ts
                    FROM alert_history
                    WHERE market_category IS NOT NULL AND market_category != ''
                    GROUP BY platform, market_id
                ) latest
                  ON latest.platform = ah.platform
                 AND latest.market_id = ah.market_id
                 AND latest.max_ts = ah.timestamp
                """
            ).fetchall()
            category_cache: Dict[Tuple[str, str], str] = {
                (r["platform"], r["market_id"]): (r["market_category"] or "").strip().lower()
                for r in category_rows
            }

            grouped: Dict[Tuple[str, str], List[sqlite3.Row]] = {}
            for row in rows:
                grouped.setdefault((row["platform"], row["market_id"]), []).append(row)

            created = 0
            scanned_markets = len(grouped)
            cooldown = timedelta(minutes=per_market_cooldown_minutes)

            for (platform, market_id), snaps in grouped.items():
                if len(snaps) < 2:
                    continue

                start_idx = 0
                last_emitted_end: Optional[datetime] = None

                for end_idx in range(1, len(snaps)):
                    end_row = snaps[end_idx]
                    end_ts = datetime.fromisoformat(end_row["timestamp"])

                    while start_idx < end_idx:
                        start_ts = datetime.fromisoformat(snaps[start_idx]["timestamp"])
                        if (end_ts - start_ts).total_seconds() <= window_minutes * 60:
                            break
                        start_idx += 1

                    if start_idx >= end_idx:
                        continue

                    start_row = snaps[start_idx]
                    start_prob = float(start_row["probability"])
                    end_prob = float(end_row["probability"])
                    change = end_prob - start_prob
                    if abs(change) < min_change_pp:
                        continue

                    if last_emitted_end and (end_ts - last_emitted_end) < cooldown:
                        continue

                    direction = 1 if change > 0 else -1
                    market_name = end_row["market_name"] or start_row["market_name"] or ""
                    category = category_cache.get((platform, market_id)) or self._infer_market_category(market_name)
                    event_key = hashlib.sha1(
                        f"{platform}|{market_id}|{start_row['timestamp']}|{end_row['timestamp']}|{change:.4f}".encode("utf-8")
                    ).hexdigest()

                    inserted = conn.execute(
                        """
                        INSERT OR IGNORE INTO market_move_events
                        (platform, market_id, market_name, market_category, start_timestamp, end_timestamp,
                         start_probability, end_probability, change_pp, direction, base_volume_24h, event_key)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            platform,
                            market_id,
                            market_name,
                            category,
                            start_row["timestamp"],
                            end_row["timestamp"],
                            start_prob,
                            end_prob,
                            float(change),
                            direction,
                            end_row["volume_24h"],
                            event_key,
                        ),
                    ).rowcount

                    if inserted:
                        created += 1
                        last_emitted_end = end_ts
                        if created >= max_events:
                            break

                if created >= max_events:
                    break

        self.set_state("truth_engine_last_move_scan", now.isoformat())
        return {"created": created, "scanned_markets": scanned_markets}

    def label_market_move_outcomes(
        self,
        horizon_minutes: int = 180,
        success_move_pp: float = 2.5,
        limit: int = 2000,
    ) -> Dict[str, int]:
        """Label unresolved move events as continuation wins/losses."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, platform, market_id, end_timestamp, end_probability, direction
                FROM market_move_events
                WHERE outcome_label IS NULL
                ORDER BY end_timestamp ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            labeled = 0
            wins = 0
            losses = 0
            now = _utcnow()

            for row in rows:
                try:
                    end_ts = datetime.fromisoformat(row["end_timestamp"])
                except ValueError:
                    continue

                if (now - end_ts).total_seconds() < horizon_minutes * 60:
                    continue

                direction = int(row["direction"])
                if direction == 0:
                    continue

                horizon_end = (end_ts + timedelta(minutes=horizon_minutes)).isoformat()
                snaps = conn.execute(
                    """
                    SELECT probability, timestamp
                    FROM market_snapshots
                    WHERE platform = ? AND market_id = ?
                      AND timestamp > ? AND timestamp <= ?
                    ORDER BY timestamp ASC
                    """,
                    (row["platform"], row["market_id"], row["end_timestamp"], horizon_end),
                ).fetchall()

                base_prob = float(row["end_probability"])
                best_move = 0.0
                time_to_hit = None
                for snap in snaps:
                    move = (float(snap["probability"]) - base_prob) * direction
                    if move > best_move:
                        best_move = move
                    if time_to_hit is None and move >= success_move_pp:
                        try:
                            hit_ts = datetime.fromisoformat(snap["timestamp"])
                            time_to_hit = (hit_ts - end_ts).total_seconds() / 60.0
                        except ValueError:
                            time_to_hit = None

                label = 1 if best_move >= success_move_pp else 0
                conn.execute(
                    """
                    UPDATE market_move_events
                    SET outcome_label = ?,
                        outcome_magnitude = ?,
                        time_to_hit_minutes = ?,
                        outcome_checked_at = ?
                    WHERE id = ?
                    """,
                    (label, float(best_move), time_to_hit, _utcnow().isoformat(), row["id"]),
                )
                labeled += 1
                if label == 1:
                    wins += 1
                else:
                    losses += 1

        return {"labeled": labeled, "wins": wins, "losses": losses}

    def get_labeled_move_performance(
        self,
        lookback_days: int = 14,
        min_samples: int = 5,
        lead_time_buckets: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """Performance summary for labeled move events."""
        if lead_time_buckets is None:
            lead_time_buckets = [15, 60, 240]

        cutoff = (_utcnow() - timedelta(days=lookback_days)).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT market_category, outcome_label, time_to_hit_minutes
                FROM market_move_events
                WHERE outcome_label IS NOT NULL
                  AND end_timestamp > ?
                """,
                (cutoff,),
            ).fetchall()

        records = [
            {
                "market_category": (r["market_category"] or "other").strip().lower(),
                "outcome": int(r["outcome_label"]),
                "time_to_hit_minutes": r["time_to_hit_minutes"],
            }
            for r in rows
        ]
        total = len(records)
        positives = sum(1 for r in records if r["outcome"] == 1)
        precision = (positives / total) if total else 0.0

        by_category: Dict[str, Dict[str, Any]] = {}
        for category in sorted({r["market_category"] for r in records}):
            matched = [r for r in records if r["market_category"] == category]
            if len(matched) < min_samples:
                continue
            tp = sum(1 for r in matched if r["outcome"] == 1)
            by_category[category] = {
                "support": len(matched),
                "true_positives": tp,
                "precision": round(tp / len(matched), 4),
            }

        by_lead_time: Dict[str, Dict[str, Any]] = {}
        for bucket in sorted(set(lead_time_buckets)):
            tp = sum(
                1 for r in records
                if r["outcome"] == 1
                and r["time_to_hit_minutes"] is not None
                and float(r["time_to_hit_minutes"]) <= bucket
            )
            by_lead_time[f"{bucket}m"] = {
                "support": total,
                "true_positives": tp,
                "precision": round(tp / total, 4) if total else 0.0,
                "recall": round(tp / positives, 4) if positives else 0.0,
            }

        return {
            "sample_size": total,
            "positives": positives,
            "overall_precision": round(precision, 4),
            "by_market_category": by_category,
            "by_lead_time": by_lead_time,
        }

    def _alert_rows_for_eval(self, lookback_days: int = 30) -> List[Dict[str, Any]]:
        cutoff = (_utcnow() - timedelta(days=lookback_days)).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT timestamp, signal_score, outcome_label
                FROM alert_history
                WHERE outcome_label IS NOT NULL
                  AND signal_score IS NOT NULL
                  AND timestamp > ?
                ORDER BY timestamp ASC
                """,
                (cutoff,),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _precision_recall_curve(rows: List[Dict[str, Any]], step: int = 5) -> List[Dict[str, float]]:
        labeled = [
            {"score": float(r["signal_score"]), "label": int(r["outcome_label"])}
            for r in rows
            if r.get("signal_score") is not None and r.get("outcome_label") is not None
        ]
        total_pos = sum(1 for r in labeled if r["label"] == 1)
        curve: List[Dict[str, float]] = []
        if not labeled or total_pos == 0:
            return curve

        for threshold in range(0, 101, max(1, step)):
            pred = [r for r in labeled if r["score"] >= threshold]
            if not pred:
                continue
            tp = sum(1 for r in pred if r["label"] == 1)
            precision = tp / len(pred)
            recall = tp / total_pos
            curve.append({
                "threshold": float(threshold),
                "support": float(len(pred)),
                "precision": round(precision, 4),
                "recall": round(recall, 4),
            })
        return curve

    @staticmethod
    def _calibration_from_rows(rows: List[Dict[str, Any]], bucket_count: int = 10) -> Dict[str, Any]:
        labeled = [
            {"score": max(0.0, min(100.0, float(r["signal_score"]))), "label": int(r["outcome_label"])}
            for r in rows
            if r.get("signal_score") is not None and r.get("outcome_label") is not None
        ]
        if not labeled:
            return {"curve": [], "ece": 0.0, "brier": 0.0}

        buckets: Dict[int, List[Dict[str, Any]]] = {i: [] for i in range(bucket_count)}
        for row in labeled:
            idx = min(bucket_count - 1, int((row["score"] / 100.0) * bucket_count))
            buckets[idx].append(row)

        total = len(labeled)
        curve = []
        ece = 0.0
        brier = 0.0
        for row in labeled:
            p = row["score"] / 100.0
            y = row["label"]
            brier += (p - y) ** 2
        brier /= total

        for idx in range(bucket_count):
            rows_b = buckets[idx]
            if not rows_b:
                continue
            count = len(rows_b)
            avg_score = sum(r["score"] for r in rows_b) / count
            win_rate = sum(r["label"] for r in rows_b) / count
            ece += abs(win_rate - (avg_score / 100.0)) * (count / total)
            curve.append({
                "bucket": idx,
                "range_low": round((idx / bucket_count) * 100.0, 1),
                "range_high": round(((idx + 1) / bucket_count) * 100.0, 1),
                "count": count,
                "avg_score": round(avg_score, 3),
                "win_rate": round(win_rate, 4),
            })

        return {"curve": curve, "ece": round(ece, 6), "brier": round(brier, 6)}

    def get_truth_engine_report(
        self,
        lookback_days: int = 30,
        min_samples: int = 8,
        precision_target: float = 0.60,
        fixed_recall: float = 0.50,
    ) -> Dict[str, Any]:
        """
        Unified truth-engine report:
        - alerts performance by signal/category/lead-time
        - all tracked move-event performance
        - calibration curve + error
        - PR curve and fixed-threshold slices
        - weekly trend for accuracy and calibration
        """
        alert_perf = self.get_labeled_alert_performance(
            lookback_days=lookback_days,
            min_samples=min_samples,
            lead_time_buckets=[15, 60, 240],
        )
        move_perf = self.get_labeled_move_performance(
            lookback_days=lookback_days,
            min_samples=min_samples,
            lead_time_buckets=[15, 60, 240],
        )
        alert_rows = self._alert_rows_for_eval(lookback_days=lookback_days)
        pr_curve = self._precision_recall_curve(alert_rows, step=5)
        calibration = self._calibration_from_rows(alert_rows, bucket_count=10)

        precision_at_fixed_recall = 0.0
        for p in pr_curve:
            if p["recall"] >= fixed_recall:
                precision_at_fixed_recall = max(precision_at_fixed_recall, p["precision"])

        recall_at_precision_target = 0.0
        for p in pr_curve:
            if p["precision"] >= precision_target:
                recall_at_precision_target = max(recall_at_precision_target, p["recall"])

        # Weekly trend
        weekly_buckets: Dict[str, List[Dict[str, Any]]] = {}
        for row in alert_rows:
            try:
                ts = datetime.fromisoformat(row["timestamp"])
            except (ValueError, KeyError):
                continue
            week_start = (ts - timedelta(days=ts.weekday())).date().isoformat()
            weekly_buckets.setdefault(week_start, []).append(row)

        weekly_trend = []
        for week_start in sorted(weekly_buckets.keys()):
            rows_w = weekly_buckets[week_start]
            pr_w = self._precision_recall_curve(rows_w, step=5)
            cal_w = self._calibration_from_rows(rows_w, bucket_count=10)
            pfr = 0.0
            rap = 0.0
            for p in pr_w:
                if p["recall"] >= fixed_recall:
                    pfr = max(pfr, p["precision"])
                if p["precision"] >= precision_target:
                    rap = max(rap, p["recall"])
            weekly_trend.append({
                "week_start": week_start,
                "precision_at_fixed_recall": round(pfr, 4),
                "recall_at_precision_target": round(rap, 4),
                "calibration_error_ece": cal_w["ece"],
                "sample_size": len(rows_w),
            })

        wow = {"precision_delta": 0.0, "ece_delta": 0.0}
        if len(weekly_trend) >= 2:
            prev = weekly_trend[-2]
            curr = weekly_trend[-1]
            wow = {
                "precision_delta": round(curr["precision_at_fixed_recall"] - prev["precision_at_fixed_recall"], 4),
                "ece_delta": round(curr["calibration_error_ece"] - prev["calibration_error_ece"], 6),
            }

        return {
            "generated_at": _utcnow().isoformat(),
            "alerts": {
                **alert_perf,
                "pr_curve": pr_curve,
                "precision_at_fixed_recall": round(precision_at_fixed_recall, 4),
                "recall_at_precision_target": round(recall_at_precision_target, 4),
                "fixed_recall_target": fixed_recall,
                "precision_target": precision_target,
            },
            "moves": move_perf,
            "calibration": calibration,
            "weekly_trend": weekly_trend,
            "wow": wow,
        }

    def get_recent_move_events(
        self,
        hours: int = 24,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Recent market move events for the evaluation dashboard."""
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT platform, market_id, market_name, market_category,
                       start_timestamp, end_timestamp, start_probability, end_probability,
                       change_pp, direction, outcome_label, outcome_magnitude, time_to_hit_minutes
                FROM market_move_events
                WHERE end_timestamp > ?
                ORDER BY end_timestamp DESC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def save_whale_story(self, address: str, condition_id: str, story_dict: Dict, insider_score: float):
        """Persist a whale story for 24h retention."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO whale_stories_cache
                (address, condition_id, story_json, insider_score, generated_at)
                VALUES (?, ?, ?, ?, ?)
            """, (address, condition_id or "", json.dumps(story_dict),
                  insider_score, _utcnow().isoformat()))

    def get_recent_whale_stories(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Retrieve whale stories generated within the last N hours."""
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT story_json, insider_score, generated_at
                FROM whale_stories_cache
                WHERE generated_at > ?
                ORDER BY insider_score DESC
            """, (cutoff,)).fetchall()
        result = []
        for row in rows:
            try:
                d = json.loads(row["story_json"])
                d["_cached_at"] = row["generated_at"]
                result.append(d)
            except Exception:
                pass
        return result

    def purge_old_whale_stories(self, hours: int = 24):
        """Remove whale stories older than N hours."""
        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()
        with self._get_conn() as conn:
            conn.execute("DELETE FROM whale_stories_cache WHERE generated_at < ?", (cutoff,))

    def get_system_stats(self) -> Dict[str, Any]:
        """Return headline stats for the dashboard header bar."""
        with self._get_conn() as conn:
            market_count = conn.execute("""
                SELECT COUNT(DISTINCT platform || market_id) as cnt
                FROM market_snapshots
                WHERE timestamp > ?
            """, ((_utcnow() - timedelta(hours=1)).isoformat(),)).fetchone()

            alert_count_24h = conn.execute("""
                SELECT COUNT(*) as cnt FROM alert_history
                WHERE timestamp > ?
            """, ((_utcnow() - timedelta(hours=24)).isoformat(),)).fetchone()

            latest_snapshot = conn.execute("""
                SELECT MAX(timestamp) as ts FROM market_snapshots
            """).fetchone()

        return {
            "markets_active": market_count["cnt"] if market_count else 0,
            "signals_24h": alert_count_24h["cnt"] if alert_count_24h else 0,
            "last_update": latest_snapshot["ts"] if latest_snapshot else None,
        }

    def get_price_history_batch(
        self,
        market_names: List[str],
        hours: int = 24,
        max_points: int = 40,
    ) -> Dict[str, List[float]]:
        """
        Return probability history for a batch of markets.

        Result: { market_name: [p0, p1, ..., pN] }  (evenly downsampled, oldest→newest)
        Each value is already on the 0-100 scale.
        Markets with fewer than 2 snapshots are omitted.
        """
        if not market_names:
            return {}

        cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()
        placeholders = ",".join("?" * len(market_names))

        with self._get_conn() as conn:
            rows = conn.execute(f"""
                SELECT market_name, timestamp, probability
                FROM market_snapshots
                WHERE market_name IN ({placeholders})
                  AND timestamp > ?
                ORDER BY market_name, timestamp ASC
            """, (*market_names, cutoff)).fetchall()

        # Group by market
        from collections import defaultdict
        buckets: Dict[str, List[float]] = defaultdict(list)
        for row in rows:
            buckets[row["market_name"]].append(float(row["probability"]))

        # Downsample each series to ≤ max_points evenly spaced values
        result: Dict[str, List[float]] = {}
        for name, pts in buckets.items():
            if len(pts) < 2:
                continue
            if len(pts) <= max_points:
                result[name] = [round(p, 1) for p in pts]
            else:
                step = len(pts) / max_points
                sampled = [pts[int(i * step)] for i in range(max_points)]
                sampled[-1] = pts[-1]  # always include the latest value
                result[name] = [round(p, 1) for p in sampled]

        return result

    # ==================== Cleanup ====================

    def cleanup_old_data(self, days: int = 7, compact: bool = False):
        """Remove old data to keep DB small."""
        cutoff = (_utcnow() - timedelta(days=days)).isoformat()
        news_cutoff = (_utcnow() - timedelta(days=2)).isoformat()  # News ages faster

        with self._get_conn() as conn:
            deleted_snapshots = conn.execute("""
                DELETE FROM market_snapshots WHERE timestamp < ?
            """, (cutoff,)).rowcount

            deleted_alerts = conn.execute("""
                DELETE FROM alert_history WHERE timestamp < ?
            """, (cutoff,)).rowcount

            deleted_orderbooks = conn.execute("""
                DELETE FROM orderbook_snapshots WHERE timestamp < ?
            """, (cutoff,)).rowcount

            deleted_moves = conn.execute("""
                DELETE FROM market_move_events WHERE end_timestamp < ?
            """, (cutoff,)).rowcount

            deleted_whale_trades = conn.execute("""
                DELETE FROM whale_trades WHERE timestamp < ?
            """, (cutoff,)).rowcount

            deleted_news = conn.execute("""
                DELETE FROM news_cache WHERE fetched_at < ?
            """, (news_cutoff,)).rowcount

        logger.info(
            f"Cleanup: removed {deleted_snapshots} snapshots, "
            f"{deleted_alerts} alerts, {deleted_orderbooks} orderbooks, "
            f"{deleted_moves} move events, "
            f"{deleted_whale_trades} whale trades, {deleted_news} news articles"
        )

        if compact:
            self.compact_database()

    def compact_database(self):
        """
        Reclaim disk space after deletions.
        WAL checkpoint + VACUUM keeps long-running local deployments bounded.
        """
        # VACUUM cannot run inside an open transaction; use a dedicated
        # autocommit connection.
        conn = sqlite3.connect(self.db_path, isolation_level=None)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.execute("PRAGMA optimize")
            conn.execute("VACUUM")
        finally:
            conn.close()

    def get_db_size_bytes(self) -> int:
        """Return current DB file size in bytes."""
        try:
            return Path(self.db_path).stat().st_size
        except OSError:
            return 0

    # ==================== Feedback Loop ====================

    def label_alert_outcomes(
        self,
        horizon_minutes: int = 180,
        success_move_pp: float = 3.0,
        limit: int = 1000,
    ) -> Dict[str, int]:
        """
        Label unresolved alerts as win/loss events.
        A "win" means the market continued in alert direction by
        `success_move_pp` within `horizon_minutes`.
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, platform, market_id, timestamp,
                       old_probability, new_probability
                FROM alert_history
                WHERE outcome_label IS NULL
                  AND old_probability IS NOT NULL
                  AND new_probability IS NOT NULL
                ORDER BY timestamp ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            labeled = 0
            wins = 0
            losses = 0
            now = _utcnow()

            for row in rows:
                try:
                    alert_time = datetime.fromisoformat(row["timestamp"])
                except ValueError:
                    continue

                # Ensure horizon has elapsed to avoid premature labels.
                if (now - alert_time).total_seconds() < horizon_minutes * 60:
                    continue

                old_p = float(row["old_probability"])
                new_p = float(row["new_probability"])
                direction = 1 if new_p > old_p else -1 if new_p < old_p else 0
                if direction == 0:
                    conn.execute(
                        """
                        UPDATE alert_history
                        SET outcome_label = 0,
                            outcome_magnitude = 0.0,
                            time_to_hit_minutes = NULL,
                            outcome_checked_at = ?
                        WHERE id = ?
                        """,
                        (_utcnow().isoformat(), row["id"]),
                    )
                    labeled += 1
                    losses += 1
                    continue

                end_time = (alert_time + timedelta(minutes=horizon_minutes)).isoformat()
                snap_rows = conn.execute(
                    """
                    SELECT probability, timestamp
                    FROM market_snapshots
                    WHERE platform = ? AND market_id = ?
                      AND timestamp > ? AND timestamp <= ?
                    ORDER BY timestamp ASC
                    """,
                    (row["platform"], row["market_id"], row["timestamp"], end_time),
                ).fetchall()

                best_move = 0.0
                time_to_hit = None
                for snap in snap_rows:
                    move = (float(snap["probability"]) - new_p) * direction
                    if move > best_move:
                        best_move = move
                    if time_to_hit is None and move >= success_move_pp:
                        try:
                            hit_time = datetime.fromisoformat(snap["timestamp"])
                            time_to_hit = (hit_time - alert_time).total_seconds() / 60.0
                        except ValueError:
                            time_to_hit = None

                label = 1 if best_move >= success_move_pp else 0
                conn.execute(
                    """
                    UPDATE alert_history
                    SET outcome_label = ?,
                        outcome_magnitude = ?,
                        time_to_hit_minutes = ?,
                        outcome_checked_at = ?
                    WHERE id = ?
                    """,
                    (
                        label,
                        float(best_move),
                        time_to_hit,
                        _utcnow().isoformat(),
                        row["id"],
                    ),
                )
                labeled += 1
                if label == 1:
                    wins += 1
                else:
                    losses += 1

        return {"labeled": labeled, "wins": wins, "losses": losses}

    def get_labeled_alert_performance(
        self,
        lookback_days: int = 14,
        min_samples: int = 5,
        lead_time_buckets: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Compute precision/recall slices from labeled alert events.
        Recall is measured as coverage of all positive outcomes in the
        labeled dataset.
        """
        if lead_time_buckets is None:
            lead_time_buckets = [15, 60, 240]

        cutoff = (_utcnow() - timedelta(days=lookback_days)).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, signal_types, market_category, outcome_label, time_to_hit_minutes
                FROM alert_history
                WHERE outcome_label IS NOT NULL
                  AND timestamp > ?
                """,
                (cutoff,),
            ).fetchall()

        records: List[Dict[str, Any]] = []
        for row in rows:
            try:
                signal_types = json.loads(row["signal_types"] or "[]")
                if not isinstance(signal_types, list):
                    signal_types = []
            except json.JSONDecodeError:
                signal_types = []
            records.append({
                "signal_types": [str(s) for s in signal_types],
                "market_category": (row["market_category"] or "").strip().lower() or "unknown",
                "outcome": int(row["outcome_label"]),
                "time_to_hit_minutes": row["time_to_hit_minutes"],
            })

        total = len(records)
        total_positive = sum(1 for r in records if r["outcome"] == 1)
        overall_precision = (total_positive / total) if total else 0.0

        def _slice_metrics(matched: List[Dict[str, Any]]) -> Dict[str, Any]:
            tp = sum(1 for r in matched if r["outcome"] == 1)
            fp = sum(1 for r in matched if r["outcome"] == 0)
            support = len(matched)
            precision = tp / support if support else 0.0
            recall = tp / total_positive if total_positive else 0.0
            return {
                "support": support,
                "true_positives": tp,
                "false_positives": fp,
                "precision": round(precision, 4),
                "recall": round(recall, 4),
            }

        by_signal: Dict[str, Dict[str, Any]] = {}
        signal_vocab = sorted({sig for r in records for sig in r["signal_types"]})
        for sig in signal_vocab:
            matched = [r for r in records if sig in r["signal_types"]]
            if len(matched) >= min_samples:
                by_signal[sig] = _slice_metrics(matched)

        by_category: Dict[str, Dict[str, Any]] = {}
        categories = sorted({r["market_category"] for r in records})
        for category in categories:
            matched = [r for r in records if r["market_category"] == category]
            if len(matched) >= min_samples:
                by_category[category] = _slice_metrics(matched)

        by_lead_time: Dict[str, Dict[str, Any]] = {}
        for bucket in sorted(set(lead_time_buckets)):
            tp = sum(
                1 for r in records
                if r["outcome"] == 1
                and r["time_to_hit_minutes"] is not None
                and float(r["time_to_hit_minutes"]) <= bucket
            )
            fp = total - tp
            precision = tp / total if total else 0.0
            recall = tp / total_positive if total_positive else 0.0
            by_lead_time[f"{bucket}m"] = {
                "support": total,
                "true_positives": tp,
                "false_positives": fp,
                "precision": round(precision, 4),
                "recall": round(recall, 4),
            }

        return {
            "sample_size": total,
            "positives": total_positive,
            "overall_precision": round(overall_precision, 4),
            "by_signal_type": by_signal,
            "by_market_category": by_category,
            "by_lead_time": by_lead_time,
        }

    # ==================== Outlook Prediction Tracking ====================

    def save_outlook_prediction(
        self,
        session_id: str,
        generated_at: str,
        market_regime: str,
        outlook_summary: str,
        dominant_themes_json: str,
        assets_json: str,
    ) -> bool:
        """Persist a new Outlook prediction batch. Ignores duplicates (session_id is UNIQUE)."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                INSERT OR IGNORE INTO outlook_predictions
                    (session_id, generated_at, market_regime, outlook_summary,
                     dominant_themes, assets_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (session_id, generated_at, market_regime, outlook_summary,
                 dominant_themes_json, assets_json),
            ).rowcount
        return rows > 0

    def get_latest_outlook_prediction(self) -> Optional[Dict[str, Any]]:
        """Return the most recent outlook prediction row, or None."""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT session_id, generated_at, market_regime, outlook_summary,
                       dominant_themes, assets_json
                FROM outlook_predictions
                ORDER BY generated_at DESC LIMIT 1
            """).fetchone()
        if not row:
            return None
        return {
            "session_id":       row["session_id"],
            "generated_at":     row["generated_at"],
            "market_regime":    row["market_regime"],
            "outlook_summary":  row["outlook_summary"],
            "dominant_themes":  json.loads(row["dominant_themes"] or "[]"),
            "assets":           json.loads(row["assets_json"] or "{}"),
        }

    def get_ungraded_predictions(self, horizon: str) -> List[Dict[str, Any]]:
        """
        Return predictions that are old enough to grade for the given horizon
        but don't yet have a grade row.
        horizon='24h'  → predictions generated more than 25 hours ago
        horizon='48h'  → predictions generated more than 49 hours ago
        """
        buffer_hours = 25 if horizon == "24h" else 49
        cutoff = (_utcnow() - timedelta(hours=buffer_hours)).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT p.session_id, p.generated_at, p.market_regime, p.assets_json
                FROM outlook_predictions p
                WHERE p.generated_at <= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM outlook_grades g
                      WHERE g.session_id = p.session_id AND g.horizon = ?
                  )
                ORDER BY p.generated_at DESC
                LIMIT 20
                """,
                (cutoff, horizon),
            ).fetchall()
        return [dict(r) for r in rows]

    def save_outlook_grade(
        self,
        session_id: str,
        horizon: str,
        graded_at: str,
        overall_score: float,
        direction_accuracy: float,
        grades_json: str,
        reflection: str = "",
    ):
        """Insert or replace a grade for a prediction/horizon pair."""
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO outlook_grades
                    (session_id, horizon, graded_at, overall_score,
                     direction_accuracy, grades_json, reflection)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, horizon, graded_at, overall_score,
                 direction_accuracy, grades_json, reflection),
            )

    def update_outlook_grade_reflection(self, grade_id: int, reflection: str):
        """Attach a Claude reflection text to an existing grade row."""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE outlook_grades SET reflection = ? WHERE id = ?",
                (reflection, grade_id),
            )

    def get_outlook_grades(self, limit: int = 30) -> List[Dict[str, Any]]:
        """Return recent grades joined with their source prediction metadata."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT g.id, g.session_id, g.horizon, g.graded_at,
                       g.overall_score, g.direction_accuracy, g.grades_json,
                       g.reflection,
                       p.generated_at AS pred_generated_at,
                       p.market_regime AS pred_regime,
                       p.dominant_themes
                FROM outlook_grades g
                JOIN outlook_predictions p ON p.session_id = g.session_id
                ORDER BY g.graded_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_outlook_track_record_stats(self) -> Dict[str, Any]:
        """
        Aggregate direction accuracy and composite score, broken down by
        horizon and by individual asset ticker.
        """
        with self._get_conn() as conn:
            # Overall per-horizon stats
            horizon_rows = conn.execute(
                """
                SELECT horizon,
                       AVG(direction_accuracy) AS avg_dir_acc,
                       AVG(overall_score)      AS avg_score,
                       COUNT(*)                AS count
                FROM outlook_grades
                GROUP BY horizon
                """
            ).fetchall()

            # All grade rows so we can compute per-asset stats in Python
            all_grade_rows = conn.execute(
                "SELECT grades_json, horizon FROM outlook_grades ORDER BY graded_at DESC LIMIT 100"
            ).fetchall()

        # Compute per-asset direction accuracy
        asset_stats: Dict[str, Dict] = {}
        for row in all_grade_rows:
            try:
                grades = json.loads(row["grades_json"])
            except Exception:
                continue
            for ticker, g in grades.items():
                if ticker not in asset_stats:
                    asset_stats[ticker] = {"correct": 0, "total": 0}
                asset_stats[ticker]["total"] += 1
                if g.get("direction_correct"):
                    asset_stats[ticker]["correct"] += 1

        per_asset = {
            t: {
                "accuracy": round(v["correct"] / v["total"], 3) if v["total"] else 0,
                "calls": v["total"],
                "correct": v["correct"],
            }
            for t, v in asset_stats.items()
        }

        horizons = {r["horizon"]: dict(r) for r in horizon_rows}
        return {"horizons": horizons, "per_asset": per_asset}

    def get_latest_outlook_reflection(self) -> str:
        """Return the most recently generated Claude reflection text."""
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT reflection FROM outlook_grades
                WHERE reflection IS NOT NULL AND reflection != ''
                ORDER BY graded_at DESC LIMIT 1
                """
            ).fetchone()
        return row["reflection"] if row else ""

    def count_outlook_predictions(self) -> int:
        """Total number of stored prediction batches."""
        with self._get_conn() as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM outlook_predictions").fetchone()
        return row["n"] if row else 0
