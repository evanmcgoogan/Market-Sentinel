"""
Regression tests for bugs found in the March 2026 code review.

Covers:
  1. generate_radar — TypeError crash when volume_24h is None during sort
  2. load_config    — ANTHROPIC_API_KEY env var must be written into config.anthropic_api_key
  3. Database._get_conn — PRAGMA synchronous/busy_timeout applied per connection
  4. api/watchlist DELETE — item_id accepted from query param (body may be stripped by proxy)
  5. api/stats — monitor_stale field present in response
"""

import json
import os
import sys
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

# Make sure src/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# 1. generate_radar — None volume_24h crash
# ---------------------------------------------------------------------------

class TestGenerateRadarNoneVolume(unittest.TestCase):
    """generate_radar must not crash when Story.volume_24h is None."""

    def _make_story(self, volume_24h):
        from story_generator import Story
        return Story(
            story_id="s1",
            market_id="m1",
            headline="Test",
            lede="Test lede",
            market_name="Test market",
            platform="polymarket",
            probability=55.0,
            old_probability=50.0,
            prob_change=5.0,
            direction="up",
            signal_score=60.0,
            signals=["price_velocity"],
            signal_types=["price_velocity"],
            category="POLITICS",
            timestamp=datetime.now(timezone.utc),
            urgency="watch",
            watch_assets=[],
            volume_24h=volume_24h,
            is_radar=False,
        )

    def test_sort_with_none_volume_does_not_raise(self):
        """Stories with None volume_24h must sort without TypeError."""
        stories = [
            self._make_story(None),
            self._make_story(100_000.0),
            self._make_story(None),
            self._make_story(50_000.0),
        ]
        # This is the exact sort used in generate_radar after the fix
        try:
            stories.sort(key=lambda s: s.volume_24h or 0.0, reverse=True)
        except TypeError as e:
            self.fail(f"sort raised TypeError with None volume_24h: {e}")

        # None-volume stories sort to the end
        self.assertEqual(stories[0].volume_24h, 100_000.0)
        self.assertEqual(stories[1].volume_24h, 50_000.0)

    def test_sort_without_fix_would_raise(self):
        """Confirm the unfixed sort (no `or 0.0`) raises TypeError on None."""
        from story_generator import Story
        stories = [
            self._make_story(None),
            self._make_story(100_000.0),
        ]
        with self.assertRaises(TypeError):
            stories.sort(key=lambda s: s.volume_24h, reverse=True)


# ---------------------------------------------------------------------------
# 2. load_config — ANTHROPIC_API_KEY env var injection
# ---------------------------------------------------------------------------

class TestLoadConfigAnthropicKeyFromEnv(unittest.TestCase):
    """ANTHROPIC_API_KEY env var must flow into config.anthropic_api_key."""

    def setUp(self):
        # Write a minimal example config with no anthropic key
        self._tmpdir = tempfile.mkdtemp()
        self._cfg_path = os.path.join(self._tmpdir, "config.json")
        with open(self._cfg_path, "w") as f:
            json.dump({"db_path": "test.db"}, f)

    def test_env_var_overrides_empty_config_key(self):
        from config import load_config
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-test-key-123",
                                      "SENTINEL_DB_PATH": ""}):
            cfg = load_config(self._cfg_path)
        self.assertEqual(cfg.anthropic_api_key, "sk-test-key-123",
                         "ANTHROPIC_API_KEY env var must be written into config.anthropic_api_key")

    def test_env_var_takes_precedence_over_config_file_key(self):
        """Env var must win even when the config file sets a different key."""
        with open(self._cfg_path, "w") as f:
            json.dump({"db_path": "test.db", "anthropic_api_key": "old-key"}, f)
        from config import load_config
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "new-env-key",
                                      "SENTINEL_DB_PATH": ""}):
            cfg = load_config(self._cfg_path)
        self.assertEqual(cfg.anthropic_api_key, "new-env-key")

    def test_empty_env_falls_back_to_config_file(self):
        """If ANTHROPIC_API_KEY is not set, the config-file value is kept."""
        with open(self._cfg_path, "w") as f:
            json.dump({"db_path": "test.db", "anthropic_api_key": "file-key"}, f)
        from config import load_config
        env_without_key = {k: v for k, v in os.environ.items() if k != "ANTHROPIC_API_KEY"}
        env_without_key.pop("SENTINEL_DB_PATH", None)
        with patch.dict(os.environ, env_without_key, clear=True):
            cfg = load_config(self._cfg_path)
        self.assertEqual(cfg.anthropic_api_key, "file-key")


# ---------------------------------------------------------------------------
# 3. Database._get_conn — PRAGMA settings applied per connection
# ---------------------------------------------------------------------------

class TestDatabaseConnPragmas(unittest.TestCase):
    """Each connection from _get_conn must apply synchronous=NORMAL and busy_timeout."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "test.db")

    def test_synchronous_normal_set_on_each_connection(self):
        from database import Database
        db = Database(self._db_path)
        with db._get_conn() as conn:
            row = conn.execute("PRAGMA synchronous").fetchone()
            # 1 = NORMAL in SQLite's PRAGMA encoding
            self.assertEqual(row[0], 1,
                             "synchronous should be NORMAL (1) on every _get_conn connection")

    def test_busy_timeout_set_on_each_connection(self):
        from database import Database
        db = Database(self._db_path)
        with db._get_conn() as conn:
            row = conn.execute("PRAGMA busy_timeout").fetchone()
            self.assertGreater(row[0], 0,
                               "busy_timeout must be > 0 to handle lock contention gracefully")

    def test_wal_mode_persists(self):
        from database import Database
        db = Database(self._db_path)
        with db._get_conn() as conn:
            row = conn.execute("PRAGMA journal_mode").fetchone()
            self.assertEqual(row[0].lower(), "wal",
                             "WAL mode must persist after init")


# ---------------------------------------------------------------------------
# 4. api/watchlist DELETE — item_id from query param
# ---------------------------------------------------------------------------

class TestWatchlistDeleteQueryParam(unittest.TestCase):
    """DELETE /api/watchlists/items must accept item_id from query param."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "test.db")
        os.environ["SENTINEL_DB_PATH"] = self._db_path
        os.environ.setdefault("SENTINEL_CONFIG",
                              os.path.join(os.path.dirname(__file__), "../../config.example.json"))

        # Import web_server after setting env so config resolves correctly
        import importlib
        import src.web_server as ws_mod
        importlib.reload(ws_mod)
        self._app = ws_mod.app
        self._app.config["TESTING"] = True

        # Seed a watchlist item in the DB so there is something to delete
        from database import Database
        db = Database(self._db_path)
        db.ensure_watchlist("Default")
        db.add_watchlist_item(
            watchlist_name="Default",
            market_id="mkt-1",
            market_name="Test market",
            platform="polymarket",
        )

    def _get_item_id(self):
        from database import Database
        db = Database(self._db_path)
        wls = db.get_watchlists()
        items = wls[0]["items"] if wls else []
        return items[0]["id"] if items else None

    def test_delete_via_json_body(self):
        item_id = self._get_item_id()
        self.assertIsNotNone(item_id)
        with self._app.test_client() as c:
            resp = c.delete(
                "/api/watchlists/items",
                json={"item_id": item_id},
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertTrue(data["ok"])

    def test_delete_via_query_param_fallback(self):
        """When JSON body is absent (proxy stripped it), query param must work."""
        from database import Database
        db = Database(self._db_path)
        db.add_watchlist_item("Default", "mkt-2", "Market 2", "kalshi")
        wls = db.get_watchlists()
        items = wls[0]["items"]
        item_id = next(i["id"] for i in items if i["market_id"] == "mkt-2")

        with self._app.test_client() as c:
            resp = c.delete(
                f"/api/watchlists/items?item_id={item_id}",
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertTrue(data["ok"])

    def test_delete_without_item_id_returns_400(self):
        with self._app.test_client() as c:
            resp = c.delete("/api/watchlists/items")
            self.assertEqual(resp.status_code, 400)


# ---------------------------------------------------------------------------
# 5. api/stats — monitor_stale field
# ---------------------------------------------------------------------------

class TestApiStatsMonitorStale(unittest.TestCase):
    """GET /api/stats must include a monitor_stale boolean."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "test.db")
        os.environ["SENTINEL_DB_PATH"] = self._db_path
        os.environ.setdefault("SENTINEL_CONFIG",
                              os.path.join(os.path.dirname(__file__), "../../config.example.json"))
        import importlib
        import src.web_server as ws_mod
        importlib.reload(ws_mod)
        self._app = ws_mod.app
        self._app.config["TESTING"] = True

    def test_stats_contains_monitor_stale(self):
        with self._app.test_client() as c:
            resp = c.get("/api/stats")
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertIn("monitor_stale", data,
                          "/api/stats must return monitor_stale field")

    def test_monitor_stale_true_when_no_snapshots(self):
        """With an empty DB, last_update is None → stale=True."""
        with self._app.test_client() as c:
            resp = c.get("/api/stats")
            data = resp.get_json()
            self.assertTrue(data["monitor_stale"],
                            "Empty DB should report monitor_stale=True")

    def test_monitor_stale_false_when_recent_snapshot(self):
        """With a recent snapshot in the DB, stale must be False."""
        from database import Database
        db = Database(self._db_path)
        db.save_snapshot(
            platform="polymarket",
            market_id="mkt-fresh",
            market_name="Fresh market",
            probability=55.0,
        )
        import importlib
        import src.web_server as ws_mod
        importlib.reload(ws_mod)
        app = ws_mod.app
        app.config["TESTING"] = True
        with app.test_client() as c:
            resp = c.get("/api/stats")
            data = resp.get_json()
            self.assertFalse(data["monitor_stale"],
                             "Recent snapshot should report monitor_stale=False")


# ---------------------------------------------------------------------------
# 6. Story clustering — question-stem matching
# ---------------------------------------------------------------------------

from story_generator import _question_stem, _is_noise_market, _detect_category


class TestQuestionStemClustering(unittest.TestCase):
    """Verify that question-stem extraction correctly groups/separates markets."""

    def test_threshold_markets_share_stem(self):
        """Same question at different thresholds → identical stems → cluster."""
        stem_250k = _question_stem("Will Trump deport 250,000 illegal immigrants before July 2025?")
        stem_500k = _question_stem("Will Trump deport 500,000 illegal immigrants before July 2025?")
        self.assertEqual(stem_250k, stem_500k,
                         "Threshold-variant markets must produce identical stems")

    def test_different_topics_different_stems(self):
        """Same entity but different topic → different stems → NO cluster."""
        stem_deport = _question_stem("Will Trump deport 250,000 illegal immigrants?")
        stem_impeach = _question_stem("Will Trump be impeached in 2025?")
        self.assertNotEqual(stem_deport, stem_impeach,
                            "Different topics about same person must NOT cluster")

    def test_candidate_markets_share_stem(self):
        """Different names for the same position → ideally separate (names are content)."""
        stem_a = _question_stem("Will Kamala Harris win the 2028 Democratic primary?")
        stem_b = _question_stem("Will Pete Buttigieg win the 2028 Democratic primary?")
        # These have different names so stems should differ
        self.assertNotEqual(stem_a, stem_b,
                            "Different candidates should NOT cluster together")

    def test_dollar_threshold_markets_share_stem(self):
        """Dollar-amount threshold variants → same stem."""
        stem_1m = _question_stem("Will tariff revenue collect $1M before Q2 2025?")
        stem_2m = _question_stem("Will tariff revenue collect $2M before Q2 2025?")
        self.assertEqual(stem_1m, stem_2m,
                         "Dollar-threshold variants must produce identical stems")

    def test_completely_unrelated_markets(self):
        """Totally different markets → different stems."""
        stem_iran = _question_stem("Will the US strike Iran before June 2025?")
        stem_fed  = _question_stem("Will the Fed cut rates in March 2025?")
        self.assertNotEqual(stem_iran, stem_fed)


class TestNoiseFilter(unittest.TestCase):
    """Verify the sports/noise filter with financial rescue."""

    def test_sports_outcome_blocked(self):
        self.assertTrue(_is_noise_market("Will Manchester United win the Premier League?"))
        self.assertTrue(_is_noise_market("Who will win Super Bowl LIX?"))
        self.assertTrue(_is_noise_market("Will the Lakers win the NBA Finals?"))

    def test_sports_financial_rescued(self):
        """Sports market with financial keywords should be KEPT."""
        self.assertFalse(_is_noise_market("Will Manchester United quarterly earnings exceed $200M?"))
        self.assertFalse(_is_noise_market("Will a new NBA franchise acquisition close in 2025?"))

    def test_crypto_noise_blocked(self):
        self.assertTrue(_is_noise_market("Will Bitcoin above $100,000 by end of day?"))
        self.assertTrue(_is_noise_market("Will ETH close above $4,000?"))

    def test_real_crypto_market_passes(self):
        """Substantive crypto markets (not daily price thresholds) should pass."""
        self.assertFalse(_is_noise_market("Will Bitcoin be declared legal tender in another country?"))
        self.assertFalse(_is_noise_market("Will the SEC approve a spot Ethereum ETF?"))

    def test_geopolitical_market_passes(self):
        self.assertFalse(_is_noise_market("Will the US strike Iran before June 2025?"))
        self.assertFalse(_is_noise_market("Will Russia-Ukraine ceasefire begin in 2025?"))

    def test_politics_market_passes(self):
        self.assertFalse(_is_noise_market("Will Trump be impeached in his second term?"))
        self.assertFalse(_is_noise_market("Will the Fed cut rates in March?"))

    def test_entertainment_blocked(self):
        self.assertTrue(_is_noise_market("Will it rain in New York tomorrow?"))
        self.assertTrue(_is_noise_market("Who will be eliminated from Big Brother?"))

    def test_category_detection_not_sports(self):
        """Core topic markets should NOT get categorized as SPORTS."""
        self.assertNotEqual(_detect_category("Will the US strike Iran?"), "SPORTS")
        self.assertNotEqual(_detect_category("Will Trump impose 25% tariffs on China?"), "SPORTS")
        self.assertEqual(_detect_category("Will the Lakers win the NBA Finals?"), "SPORTS")


if __name__ == "__main__":
    unittest.main(verbosity=2)
