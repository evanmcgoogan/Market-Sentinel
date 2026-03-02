"""Tests for database module."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database import Database


class TestDatabase(unittest.TestCase):
    """Test Database operations."""

    def setUp(self):
        """Create a temp database for each test."""
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db = Database(self.db_path)

    def tearDown(self):
        """Clean up temp database."""
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_save_and_get_snapshot(self):
        """Test saving and retrieving a snapshot."""
        self.db.save_snapshot(
            platform="polymarket",
            market_id="test-123",
            market_name="Test Market",
            probability=65.0,
            volume=10000.0,
            volume_24h=500.0,
            liquidity=2000.0,
        )

        latest = self.db.get_latest_snapshot("polymarket", "test-123")
        self.assertIsNotNone(latest)
        self.assertEqual(latest["platform"], "polymarket")
        self.assertEqual(latest["market_id"], "test-123")
        self.assertAlmostEqual(latest["probability"], 65.0)
        self.assertAlmostEqual(latest["volume_24h"], 500.0)

    def test_get_recent_snapshots(self):
        """Test retrieving recent snapshots within time window."""
        # Save a few snapshots
        for i in range(3):
            self.db.save_snapshot(
                platform="kalshi",
                market_id="k-1",
                market_name="Kalshi Test",
                probability=50.0 + i,
            )

        snapshots = self.db.get_recent_snapshots("kalshi", "k-1", minutes=60)
        self.assertEqual(len(snapshots), 3)
        # Should be ordered ASC
        self.assertAlmostEqual(snapshots[0]["probability"], 50.0)
        self.assertAlmostEqual(snapshots[2]["probability"], 52.0)

    def test_get_baseline_volume_average(self):
        """Test that baseline volume returns average, not just latest."""
        self.db.save_snapshot(
            platform="polymarket",
            market_id="vol-test",
            market_name="Volume Test",
            probability=50.0,
            volume_24h=100.0,
        )
        self.db.save_snapshot(
            platform="polymarket",
            market_id="vol-test",
            market_name="Volume Test",
            probability=51.0,
            volume_24h=300.0,
        )

        baseline = self.db.get_baseline_volume("polymarket", "vol-test", hours=24)
        self.assertIsNotNone(baseline)
        # Average of 100 and 300 = 200
        self.assertAlmostEqual(baseline, 200.0)

    def test_get_baseline_volume_no_data(self):
        """Test baseline volume with no data returns None."""
        result = self.db.get_baseline_volume("polymarket", "nonexistent", hours=24)
        self.assertIsNone(result)

    def test_record_and_get_alert(self):
        """Test alert recording and cooldown checking."""
        self.db.record_alert(
            platform="polymarket",
            market_id="alert-1",
            market_name="Alert Market",
            signal_score=75.0,
            reasons=["price velocity", "volume shock"],
            old_probability=40.0,
            new_probability=55.0,
        )

        last_time = self.db.get_last_alert_time("polymarket", "alert-1")
        self.assertIsNotNone(last_time)
        # Should be very recent
        delta = (datetime.utcnow() - last_time).total_seconds()
        self.assertLess(delta, 5)

    def test_count_recent_alerts(self):
        """Test counting recent alerts."""
        for i in range(5):
            self.db.record_alert(
                platform="kalshi",
                market_id=f"count-{i}",
                market_name=f"Count Market {i}",
                signal_score=50.0,
                reasons=["test"],
                old_probability=None,
                new_probability=None,
            )

        count = self.db.count_recent_alerts(minutes=60)
        self.assertEqual(count, 5)

    def test_state_persistence(self):
        """Test key-value state storage."""
        self.db.set_state("test_key", {"foo": "bar", "count": 42})
        result = self.db.get_state("test_key")
        self.assertEqual(result, {"foo": "bar", "count": 42})

    def test_state_default(self):
        """Test state returns default for missing key."""
        result = self.db.get_state("nonexistent", default="fallback")
        self.assertEqual(result, "fallback")

    def test_cleanup_old_data(self):
        """Test that cleanup removes old data."""
        # Save a snapshot
        self.db.save_snapshot(
            platform="polymarket",
            market_id="cleanup-test",
            market_name="Cleanup Test",
            probability=50.0,
        )

        # Cleanup with 0 days should remove everything
        self.db.cleanup_old_data(days=0)
        latest = self.db.get_latest_snapshot("polymarket", "cleanup-test")
        self.assertIsNone(latest)

    def test_context_manager_reraises(self):
        """Test that database errors are re-raised to callers."""
        # Try to insert invalid data that will cause an error
        with self.assertRaises(Exception):
            # Force an error by using a bad query
            with self.db._get_conn() as conn:
                conn.execute("INSERT INTO nonexistent_table VALUES (1)")


class TestDatabaseLatestSnapshot(unittest.TestCase):
    """Test get_latest_snapshot specifically."""

    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db = Database(self.db_path)

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_returns_none_for_missing(self):
        """Test that missing market returns None."""
        result = self.db.get_latest_snapshot("polymarket", "does-not-exist")
        self.assertIsNone(result)

    def test_returns_most_recent(self):
        """Test that it returns the most recent snapshot."""
        for prob in [40.0, 50.0, 60.0]:
            self.db.save_snapshot(
                platform="polymarket",
                market_id="latest-test",
                market_name="Latest Test",
                probability=prob,
            )

        latest = self.db.get_latest_snapshot("polymarket", "latest-test")
        self.assertIsNotNone(latest)
        self.assertAlmostEqual(latest["probability"], 60.0)


if __name__ == "__main__":
    unittest.main()
