"""Regression tests for reliability fixes and scoring feedback loop."""

import asyncio
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import Config, SignalThresholds, WhaleConfig
from database import Database
from main import MarketSentinel
from models import Market, Platform
from signals import SignalDetector
from whale_tracker import WhaleTracker, GLOBAL_WHALE_MARKET_ID


def make_market(
    market_id: str,
    probability: float = 50.0,
    category: str = "politics",
) -> Market:
    return Market(
        platform=Platform.POLYMARKET,
        market_id=market_id,
        slug=market_id,
        name=f"Market {market_id}",
        category=category,
        probability=probability,
        volume_24h=20000.0,
        liquidity=15000.0,
        raw_data={"tokens": [{"outcome": "Yes", "token_id": f"token-{market_id}"}]},
    )


class FakeNewsMonitor:
    def check_news_coverage(self, market_name: str, market_description: str, lookback_hours: int):
        return {"has_news": False, "search_terms": ["test"]}


class FakePolyClient:
    async def refresh_market_prices(self, markets):
        for m in markets:
            m.probability += 7.0
        return markets


class FakeKalshiClient:
    async def refresh_market_prices(self, markets):
        for m in markets:
            m.probability -= 3.0
        return markets


class TestReliabilityAndFeedback(unittest.TestCase):
    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db = Database(self.db_path)

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_no_news_signal_is_active(self):
        cfg = SignalThresholds(no_news_min_price_change=3.0)
        detector = SignalDetector(cfg, self.db, news_monitor=FakeNewsMonitor())

        market = make_market("nn1", probability=60.0)
        self.db.save_snapshot(
            platform=market.platform_str,
            market_id=market.market_id,
            market_name=market.name,
            probability=50.0,
            volume_24h=1000.0,
            liquidity=500.0,
        )
        self.db.save_snapshot(
            platform=market.platform_str,
            market_id=market.market_id,
            market_name=market.name,
            probability=52.0,
            volume_24h=1100.0,
            liquidity=500.0,
        )

        signals = detector.detect_signals(market)
        signal_types = {s.signal_type for s in signals}
        self.assertIn("no_news_move", signal_types)

    def test_whale_tracker_stub_returns_empty(self):
        """WhaleTracker is now a no-op stub — all whale logic lives in WhaleBrain."""
        cfg = WhaleConfig()
        tracker = WhaleTracker(cfg, self.db)

        activity = tracker.get_recent_whale_activity("any-market", minutes=120)
        self.assertFalse(activity["has_whale_activity"])
        self.assertEqual(activity["trade_count"], 0)
        self.assertEqual(activity["total_volume"], 0.0)
        self.assertEqual(activity["smart_money_trades"], 0)

    def test_compaction_strategy_runs(self):
        called = {"compact": False}
        original = self.db.compact_database

        def wrapped():
            called["compact"] = True
            return original()

        self.db.compact_database = wrapped  # type: ignore[assignment]
        self.db.cleanup_old_data(days=0, compact=True)
        self.assertTrue(called["compact"])

    def test_outcome_labeling_and_metrics(self):
        market_id = "m-feedback"
        platform = "polymarket"

        # Alert record.
        self.db.record_alert(
            platform=platform,
            market_id=market_id,
            market_name="Feedback Market",
            signal_score=70.0,
            reasons=["Sudden move"],
            old_probability=40.0,
            new_probability=50.0,
            signal_types=["price_velocity"],
            market_category="politics",
        )

        # Make alert old enough and create in-horizon snapshots.
        t0 = datetime.utcnow() - timedelta(hours=2)
        with self.db._get_conn() as conn:
            conn.execute(
                "UPDATE alert_history SET timestamp=? WHERE platform=? AND market_id=?",
                (t0.isoformat(), platform, market_id),
            )

        self.db.save_snapshot(
            platform=platform,
            market_id=market_id,
            market_name="Feedback Market",
            probability=53.0,
            volume_24h=1000.0,
            liquidity=500.0,
        )
        self.db.save_snapshot(
            platform=platform,
            market_id=market_id,
            market_name="Feedback Market",
            probability=55.0,
            volume_24h=1100.0,
            liquidity=550.0,
        )
        with self.db._get_conn() as conn:
            rows = conn.execute(
                "SELECT id FROM market_snapshots WHERE platform=? AND market_id=? ORDER BY id ASC",
                (platform, market_id),
            ).fetchall()
            conn.execute(
                "UPDATE market_snapshots SET timestamp=? WHERE id=?",
                ((t0 + timedelta(minutes=10)).isoformat(), rows[0]["id"]),
            )
            conn.execute(
                "UPDATE market_snapshots SET timestamp=? WHERE id=?",
                ((t0 + timedelta(minutes=20)).isoformat(), rows[1]["id"]),
            )

        labeled = self.db.label_alert_outcomes(horizon_minutes=60, success_move_pp=3.0)
        self.assertEqual(labeled["labeled"], 1)
        self.assertEqual(labeled["wins"], 1)

        metrics = self.db.get_labeled_alert_performance(lookback_days=30, min_samples=1)
        self.assertEqual(metrics["sample_size"], 1)
        self.assertGreaterEqual(metrics["overall_precision"], 1.0)
        self.assertIn("price_velocity", metrics["by_signal_type"])
        self.assertIn("politics", metrics["by_market_category"])

    def test_auto_tune_thresholds(self):
        detector = SignalDetector(SignalThresholds(), self.db)
        before = detector.config.alert_threshold

        updates = detector.auto_tune_thresholds(
            performance={"precision": 0.35, "recall": 0.60},
            target_precision=0.60,
            min_recall=0.30,
            step_fraction=0.05,
        )
        self.assertTrue(updates)
        self.assertGreater(detector.config.alert_threshold, before)

        after_tighten = detector.config.alert_threshold
        updates2 = detector.auto_tune_thresholds(
            performance={"precision": 0.75, "recall": 0.10},
            target_precision=0.60,
            min_recall=0.30,
            step_fraction=0.05,
        )
        self.assertTrue(updates2)
        self.assertLess(detector.config.alert_threshold, after_tighten)

    def test_move_event_detection_labeling_and_idempotency(self):
        market_id = "move-1"
        platform = "polymarket"
        now = datetime.utcnow()

        # Seed snapshots that create a large move, then continuation.
        points = [
            (now - timedelta(hours=4), 40.0),
            (now - timedelta(hours=3, minutes=30), 44.5),
            (now - timedelta(hours=3, minutes=10), 47.2),
        ]
        for ts, prob in points:
            self.db.save_snapshot(
                platform=platform,
                market_id=market_id,
                market_name="Will inflation cool by Q3?",
                probability=prob,
                volume_24h=12000.0,
                liquidity=5000.0,
            )
        with self.db._get_conn() as conn:
            rows = conn.execute(
                "SELECT id FROM market_snapshots WHERE platform=? AND market_id=? ORDER BY id ASC",
                (platform, market_id),
            ).fetchall()
            for idx, row in enumerate(rows):
                conn.execute(
                    "UPDATE market_snapshots SET timestamp=? WHERE id=?",
                    (points[idx][0].isoformat(), row["id"]),
                )

        # Add category hint from alert history for attribution.
        self.db.record_alert(
            platform=platform,
            market_id=market_id,
            market_name="Will inflation cool by Q3?",
            signal_score=68.0,
            reasons=["Macro repricing"],
            old_probability=40.0,
            new_probability=44.5,
            signal_types=["price_velocity"],
            market_category="markets",
        )
        with self.db._get_conn() as conn:
            conn.execute(
                "UPDATE alert_history SET timestamp=? WHERE platform=? AND market_id=?",
                ((now - timedelta(hours=3, minutes=35)).isoformat(), platform, market_id),
            )

        first = self.db.detect_market_move_events(
            window_minutes=60,
            min_change_pp=2.0,
            scan_minutes=12 * 60,
            per_market_cooldown_minutes=20,
            max_events=100,
        )
        self.assertGreaterEqual(first["created"], 1)
        self.assertEqual(first["scanned_markets"], 1)

        labeled = self.db.label_market_move_outcomes(
            horizon_minutes=120,
            success_move_pp=2.5,
            limit=100,
        )
        self.assertGreaterEqual(labeled["labeled"], 1)
        self.assertGreaterEqual(labeled["wins"], 1)

        events = self.db.get_recent_move_events(hours=24, limit=10)
        self.assertTrue(events)
        self.assertEqual(events[0]["market_category"], "markets")
        self.assertIn(events[0]["outcome_label"], (0, 1))

        second = self.db.detect_market_move_events(
            window_minutes=60,
            min_change_pp=2.0,
            scan_minutes=12 * 60,
            per_market_cooldown_minutes=20,
            max_events=100,
        )
        self.assertEqual(second["created"], 0)

    def test_truth_engine_report_has_calibration_and_slices(self):
        now = datetime.utcnow()

        labeled_alerts = [
            ("a1", now - timedelta(days=20), 86.0, 1, 18.0, ["price_velocity"], "politics"),
            ("a2", now - timedelta(days=19), 72.0, 0, None, ["whale_activity"], "politics"),
            ("a3", now - timedelta(days=6), 91.0, 1, 25.0, ["price_velocity"], "markets"),
            ("a4", now - timedelta(days=5), 34.0, 0, None, ["no_news_move"], "markets"),
        ]
        for market_id, ts, score, label, tth, signal_types, category in labeled_alerts:
            self.db.record_alert(
                platform="polymarket",
                market_id=market_id,
                market_name=f"Market {market_id}",
                signal_score=score,
                reasons=["Synthetic labeled row"],
                old_probability=45.0,
                new_probability=55.0,
                signal_types=signal_types,
                market_category=category,
            )
            with self.db._get_conn() as conn:
                conn.execute(
                    """
                    UPDATE alert_history
                    SET timestamp=?, outcome_label=?, outcome_magnitude=?, time_to_hit_minutes=?, outcome_checked_at=?
                    WHERE platform=? AND market_id=?
                    """,
                    (
                        ts.isoformat(),
                        label,
                        3.2 if label == 1 else 0.8,
                        tth,
                        (ts + timedelta(hours=3)).isoformat(),
                        "polymarket",
                        market_id,
                    ),
                )

        move_rows = [
            ("m1", "Will CPI cool in June?", "markets", now - timedelta(days=7), 48.0, 53.0, 1, 3.0, 35.0),
            ("m2", "Will Senate pass bill?", "politics", now - timedelta(days=4), 57.0, 52.0, 0, 1.1, None),
        ]
        with self.db._get_conn() as conn:
            for market_id, name, cat, end_ts, start_p, end_p, label, mag, tth in move_rows:
                start_ts = end_ts - timedelta(minutes=40)
                conn.execute(
                    """
                    INSERT INTO market_move_events
                    (platform, market_id, market_name, market_category, start_timestamp, end_timestamp,
                     start_probability, end_probability, change_pp, direction, base_volume_24h, event_key,
                     outcome_label, outcome_magnitude, time_to_hit_minutes, outcome_checked_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "polymarket",
                        market_id,
                        name,
                        cat,
                        start_ts.isoformat(),
                        end_ts.isoformat(),
                        start_p,
                        end_p,
                        end_p - start_p,
                        1 if end_p >= start_p else -1,
                        12000.0,
                        f"seed-{market_id}",
                        label,
                        mag,
                        tth,
                        (end_ts + timedelta(hours=3)).isoformat(),
                    ),
                )

        report = self.db.get_truth_engine_report(
            lookback_days=30,
            min_samples=1,
            precision_target=0.60,
            fixed_recall=0.50,
        )

        self.assertGreaterEqual(report["alerts"]["sample_size"], 4)
        self.assertTrue(report["alerts"]["pr_curve"])
        self.assertIn("price_velocity", report["alerts"]["by_signal_type"])
        self.assertIn("markets", report["alerts"]["by_market_category"])
        self.assertIn("60m", report["alerts"]["by_lead_time"])
        self.assertTrue(report["calibration"]["curve"])
        self.assertGreaterEqual(report["calibration"]["ece"], 0.0)
        self.assertIn("markets", report["moves"]["by_market_category"])
        self.assertIn("60m", report["moves"]["by_lead_time"])
        self.assertGreaterEqual(len(report["weekly_trend"]), 2)
        self.assertIn("precision_delta", report["wow"])
        self.assertIn("ece_delta", report["wow"])

    def test_feedback_loop_persists_truth_report_state(self):
        cfg = Config()
        cfg.db_path = self.db_path
        cfg.autotune.enabled = False

        sentinel = MarketSentinel(cfg)
        sentinel.db = self.db

        self.db.record_alert(
            platform="polymarket",
            market_id="loop-1",
            market_name="Loop Market",
            signal_score=62.0,
            reasons=["Move"],
            old_probability=45.0,
            new_probability=52.0,
            signal_types=["price_velocity"],
            market_category="politics",
        )
        t0 = datetime.utcnow() - timedelta(hours=4)
        with self.db._get_conn() as conn:
            conn.execute(
                "UPDATE alert_history SET timestamp=? WHERE platform=? AND market_id=?",
                (t0.isoformat(), "polymarket", "loop-1"),
            )

        sentinel._last_feedback = datetime.min.replace(tzinfo=timezone.utc)
        sentinel._maybe_feedback_loop()

        truth_report = self.db.get_state("truth_engine_report", default=None)
        compat = self.db.get_state("signal_performance_metrics", default=None)
        self.assertIsNotNone(truth_report)
        self.assertIsNotNone(compat)
        self.assertIn("alerts", truth_report)
        self.assertIn("moves", truth_report)

    def test_live_price_refresh_updates_cached_markets(self):
        cfg = Config()
        cfg.db_path = self.db_path
        sentinel = MarketSentinel(cfg)
        sentinel.db = self.db
        sentinel.polymarket = FakePolyClient()
        sentinel.kalshi = FakeKalshiClient()
        sentinel._cached_markets = {
            "polymarket": [make_market("pm1", probability=40.0)],
            "kalshi": [Market(
                platform=Platform.KALSHI,
                market_id="k1",
                slug="k1",
                name="Kalshi 1",
                probability=60.0,
                volume_24h=1000.0,
                liquidity=1000.0,
                raw_data={},
            )],
        }

        asyncio.run(sentinel._update_prices())
        self.assertAlmostEqual(sentinel._cached_markets["polymarket"][0].probability, 47.0)
        self.assertAlmostEqual(sentinel._cached_markets["kalshi"][0].probability, 57.0)


if __name__ == "__main__":
    unittest.main()
