"""Tests for watchlists, thesis threads, and decision-workflow DB helpers."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database import Database


class TestDecisionWorkflowDatabase(unittest.TestCase):
    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db = Database(self.db_path)

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_watchlist_roundtrip(self):
        added = self.db.add_watchlist_item(
            watchlist_name="Core",
            market_id="m-1",
            market_name="Will inflation fall below 2%?",
            platform="polymarket",
            category="markets",
        )
        self.assertTrue(added)

        # Duplicate should be idempotent.
        added_dup = self.db.add_watchlist_item(
            watchlist_name="Core",
            market_id="m-1",
            market_name="Will inflation fall below 2%?",
            platform="polymarket",
            category="markets",
        )
        self.assertFalse(added_dup)

        watchlists = self.db.get_watchlists()
        self.assertEqual(len(watchlists), 1)
        self.assertEqual(watchlists[0]["name"], "Core")
        self.assertEqual(len(watchlists[0]["items"]), 1)

    def test_thesis_follow_and_note(self):
        thread_id = self.db.follow_thesis(
            thesis_key="markets-abc123",
            title="Fed easing thesis",
            category="MARKETS",
            note="Started tracking this thesis.",
            payload={"market_name": "Will Fed cut by June?"},
        )
        self.assertGreater(thread_id, 0)

        ok = self.db.add_thesis_note("markets-abc123", "CPI print is next catalyst.")
        self.assertTrue(ok)

        threads = self.db.get_thesis_threads(limit=5)
        self.assertEqual(len(threads), 1)
        self.assertEqual(threads[0]["thesis_key"], "markets-abc123")
        self.assertGreaterEqual(len(threads[0]["updates"]), 2)

    def test_recent_alert_candidates(self):
        self.db.record_alert(
            platform="polymarket",
            market_id="m-2",
            market_name="Will ceasefire hold this quarter?",
            signal_score=66.0,
            reasons=["Volume spike"],
            old_probability=45.0,
            new_probability=58.0,
            signal_types=["volume_shock"],
            market_category="conflict",
        )
        rows = self.db.get_recent_alert_candidates(category="conflict", days=1, limit=20)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market_name"], "Will ceasefire hold this quarter?")

    def test_auto_link_alert_to_followed_thesis(self):
        thesis_key = "markets-fedcut"
        self.db.follow_thesis(
            thesis_key=thesis_key,
            title="Fed Cut Thesis",
            category="MARKETS",
            note="Track Fed easing odds",
            payload={"market_name": "Will Fed cut rates by June?"},
        )

        matched = self.db.link_alert_to_followed_thesis(
            market_name="Will Fed cut rates in June 2026?",
            category="MARKETS",
            platform="polymarket",
            market_id="fed-june-2026",
            signal_score=72.0,
            signal_types=["price_velocity", "volume_shock"],
        )
        self.assertEqual(matched, thesis_key)

        threads = self.db.get_thesis_threads(limit=5)
        self.assertEqual(len(threads), 1)
        signal_updates = [u for u in threads[0]["updates"] if u["event_type"] == "signal"]
        self.assertGreaterEqual(len(signal_updates), 1)

    def test_auto_link_requires_similarity(self):
        self.db.follow_thesis(
            thesis_key="geo-ukraine",
            title="Ukraine Ceasefire Thesis",
            category="CONFLICT",
            note="Track ceasefire probabilities",
            payload={"market_name": "Will Ukraine and Russia agree ceasefire?"},
        )
        matched = self.db.link_alert_to_followed_thesis(
            market_name="Will Nvidia beat revenue estimates?",
            category="TECHNOLOGY",
            platform="polymarket",
            market_id="nvda-rev",
            signal_score=61.0,
            signal_types=["price_velocity"],
        )
        self.assertIsNone(matched)

    def test_watchlist_enriched_fields(self):
        self.db.add_watchlist_item(
            watchlist_name="Core",
            market_id="m-enriched",
            market_name="Will CPI fall below 3%?",
            platform="polymarket",
            category="markets",
        )
        self.db.save_snapshot(
            platform="polymarket",
            market_id="m-enriched",
            market_name="Will CPI fall below 3%?",
            probability=55.0,
            volume_24h=10000.0,
            liquidity=5000.0,
        )
        self.db.save_snapshot(
            platform="polymarket",
            market_id="m-enriched",
            market_name="Will CPI fall below 3%?",
            probability=60.5,
            volume_24h=12000.0,
            liquidity=5200.0,
        )
        self.db.record_alert(
            platform="polymarket",
            market_id="m-enriched",
            market_name="Will CPI fall below 3%?",
            signal_score=67.0,
            reasons=["Macro repricing"],
            old_probability=54.0,
            new_probability=60.5,
            signal_types=["price_velocity", "no_news_move"],
            market_category="markets",
        )

        # Backdate first snapshot so 24h delta can be computed.
        with self.db._get_conn() as conn:
            rows = conn.execute(
                "SELECT id FROM market_snapshots WHERE platform='polymarket' AND market_id='m-enriched' ORDER BY id ASC"
            ).fetchall()
            old_ts = (datetime.utcnow() - timedelta(hours=26)).isoformat()
            conn.execute("UPDATE market_snapshots SET timestamp=? WHERE id=?", (old_ts, rows[0]["id"]))

        watchlists = self.db.get_watchlists_enriched(max_items_per_watchlist=10)
        self.assertEqual(len(watchlists), 1)
        wl = watchlists[0]
        self.assertEqual(wl["name"], "Core")
        self.assertEqual(wl["item_count"], 1)
        item = wl["items"][0]
        self.assertAlmostEqual(item["latest_probability"], 60.5)
        self.assertIsNotNone(item["delta_24h_pp"])
        self.assertEqual(item["last_signal_score"], 67.0)
        self.assertIn("price_velocity", item["last_signal_types"])
        self.assertGreater(item["decision_priority"], 0.0)

    def test_thesis_action_event_logged(self):
        self.db.follow_thesis(
            thesis_key="markets-cpi",
            title="CPI Cooling Thesis",
            category="MARKETS",
            note="Start following CPI thesis.",
            payload={"market_name": "Will CPI fall below 3%?"},
        )
        ok = self.db.add_thesis_action(
            thesis_key="markets-cpi",
            action="Set invalidation guardrail at 49%",
            rationale="Protect against sudden reversal",
        )
        self.assertTrue(ok)

        threads = self.db.get_thesis_threads(limit=5)
        self.assertEqual(len(threads), 1)
        self.assertTrue(any(u["event_type"] == "action" for u in threads[0]["updates"]))

    def test_thesis_copilot_outputs_workflow_fields(self):
        self.db.follow_thesis(
            thesis_key="markets-fed",
            title="Fed Cut Thesis",
            category="MARKETS",
            note="Track rates repricing around FOMC and CPI.",
            payload={"market_name": "Will the Fed cut rates by June?"},
        )
        self.db.add_watchlist_item(
            watchlist_name="Macro",
            market_id="fed-june",
            market_name="Will the Fed cut rates by June?",
            platform="polymarket",
            category="markets",
        )
        self.db.record_alert(
            platform="polymarket",
            market_id="fed-june",
            market_name="Will the Fed cut rates by June?",
            signal_score=74.0,
            reasons=["Rates repricing"],
            old_probability=46.0,
            new_probability=53.0,
            signal_types=["price_velocity", "cross_market_divergence", "no_news_move"],
            market_category="markets",
        )
        self.db.record_alert(
            platform="polymarket",
            market_id="fed-sept",
            market_name="Will CPI surprise lower before FOMC?",
            signal_score=71.0,
            reasons=["Macro catalyst"],
            old_probability=44.0,
            new_probability=51.0,
            signal_types=["price_velocity", "whale_activity"],
            market_category="markets",
        )

        threads = self.db.get_thesis_copilot_threads(limit=5, alert_lookback_days=30)
        self.assertEqual(len(threads), 1)
        cp = threads[0]["copilot"]
        self.assertIn("summary", cp)
        self.assertTrue(cp["catalysts"])
        self.assertTrue(cp["falsifiers"])
        self.assertEqual(len(cp["scenario_tree"]), 3)
        self.assertTrue(cp["next_best_actions"])
        self.assertGreaterEqual(cp["urgency_score"], 0.0)
        self.assertGreaterEqual(cp["linked_watchlists_count"], 1)


if __name__ == "__main__":
    unittest.main()
