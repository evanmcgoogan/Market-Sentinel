"""Tests for signal detection logic."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models import Market, Platform, Signal, MarketPair
from config import SignalThresholds
from database import Database
from signals import SignalDetector, MarketMatcher


def make_market(
    name="Test Market",
    platform=Platform.POLYMARKET,
    market_id="test-123",
    probability=50.0,
    volume_24h=50000.0,
    liquidity=20000.0,
    end_date=None,
    **kwargs,
) -> Market:
    """Helper to create test markets."""
    return Market(
        platform=platform,
        market_id=market_id,
        slug=market_id,
        name=name,
        probability=probability,
        volume_24h=volume_24h,
        liquidity=liquidity,
        end_date=end_date,
        **kwargs,
    )


class TestSignalDetector(unittest.TestCase):
    """Test signal detection heuristics."""

    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db = Database(self.db_path)
        self.config = SignalThresholds()
        self.detector = SignalDetector(self.config, self.db)

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def _seed_snapshots(self, market_id, platform, probabilities):
        """Seed the database with snapshot data."""
        for prob in probabilities:
            self.db.save_snapshot(
                platform=platform,
                market_id=market_id,
                market_name="Test",
                probability=prob,
                volume_24h=1000.0,
                liquidity=500.0,
            )

    def test_price_velocity_detected(self):
        """Test that a large price move triggers price_velocity signal."""
        market = make_market(probability=60.0)
        # Seed with old probability of 50
        self._seed_snapshots("test-123", "polymarket", [50.0, 52.0])

        signal = self.detector._detect_price_velocity(market)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.signal_type, "price_velocity")
        self.assertGreater(signal.strength, 0)

    def test_price_velocity_not_triggered_small_change(self):
        """Test that a small price change doesn't trigger."""
        market = make_market(probability=51.0)
        self._seed_snapshots("test-123", "polymarket", [50.0, 50.5])

        signal = self.detector._detect_price_velocity(market)
        self.assertIsNone(signal)

    def test_price_velocity_insufficient_snapshots(self):
        """Test that detection requires at least 2 snapshots."""
        market = make_market(probability=70.0)
        # Only one snapshot
        self._seed_snapshots("test-123", "polymarket", [50.0])

        signal = self.detector._detect_price_velocity(market)
        self.assertIsNone(signal)

    def test_volume_shock_detected(self):
        """Test volume shock detection when volume spikes."""
        market = make_market(volume_24h=15000.0)

        # Seed baseline of ~5000
        for _ in range(5):
            self.db.save_snapshot(
                platform="polymarket",
                market_id="test-123",
                market_name="Test",
                probability=50.0,
                volume_24h=5000.0,
            )

        signal = self.detector._detect_volume_shock(market)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.signal_type, "volume_shock")

    def test_volume_shock_not_triggered_normal_volume(self):
        """Test that normal volume doesn't trigger."""
        market = make_market(volume_24h=5500.0)

        for _ in range(5):
            self.db.save_snapshot(
                platform="polymarket",
                market_id="test-123",
                market_name="Test",
                probability=50.0,
                volume_24h=5000.0,
            )

        signal = self.detector._detect_volume_shock(market)
        self.assertIsNone(signal)

    def test_thin_liquidity_detected(self):
        """Test thin liquidity detection (both vol and liq must be low)."""
        market = make_market(
            probability=60.0,
            volume_24h=5000.0,
            liquidity=3000.0,
        )
        self._seed_snapshots("test-123", "polymarket", [50.0, 52.0])

        signal = self.detector._detect_thin_liquidity_jump(market)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.signal_type, "thin_liquidity_jump")

    def test_thin_liquidity_not_triggered_high_volume(self):
        """Test that high volume market is not considered thin."""
        market = make_market(
            probability=60.0,
            volume_24h=50000.0,  # High volume
            liquidity=3000.0,
        )
        self._seed_snapshots("test-123", "polymarket", [50.0, 52.0])

        signal = self.detector._detect_thin_liquidity_jump(market)
        self.assertIsNone(signal)

    def test_cross_market_divergence(self):
        """Test divergence detection between platforms."""
        market = make_market(probability=60.0)
        paired = make_market(
            platform=Platform.KALSHI,
            market_id="kalshi-123",
            probability=45.0,  # 15pp gap
        )

        signal = self.detector._detect_cross_market_divergence(market, paired)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.signal_type, "cross_market_divergence")

    def test_cross_market_no_divergence(self):
        """Test no signal when platforms agree."""
        market = make_market(probability=60.0)
        paired = make_market(
            platform=Platform.KALSHI,
            market_id="kalshi-123",
            probability=58.0,  # Only 2pp gap
        )

        signal = self.detector._detect_cross_market_divergence(market, paired)
        self.assertIsNone(signal)

    def test_signal_score_calculation(self):
        """Test composite score from multiple signals."""
        signals = [
            Signal("price_velocity", "test", strength=30.0),
            Signal("volume_shock", "test", strength=20.0),
        ]
        market = make_market()

        score = self.detector.calculate_signal_score(signals, market)
        # Sum is 50, but capped at 85 before boost, no end_date so no boost
        self.assertAlmostEqual(score, 50.0)

    def test_signal_score_capped_at_85_before_boost(self):
        """Test that base score caps at 85 before late-stage boost."""
        # Use same signal type to avoid triggering correlation bonus
        signals = [
            Signal("a", "test", strength=40.0),
            Signal("a", "test", strength=35.0),
            Signal("a", "test", strength=30.0),
        ]
        market = make_market()  # No end_date = no boost

        score = self.detector.calculate_signal_score(signals, market)
        # Raw sum = 105, but capped at 85, 1 unique type so no correlation bonus
        self.assertAlmostEqual(score, 85.0)

    def test_late_stage_boost(self):
        """Test that late-stage boost increases score."""
        market = make_market(
            end_date=datetime.now(timezone.utc) + timedelta(days=1),  # Resolves tomorrow
        )

        base = 60.0
        boosted = self.detector._apply_late_stage_boost(base, market)
        self.assertGreater(boosted, base)

    def test_no_boost_far_resolution(self):
        """Test no boost when resolution is far away."""
        market = make_market(
            end_date=datetime.now(timezone.utc) + timedelta(days=30),
        )

        base = 60.0
        boosted = self.detector._apply_late_stage_boost(base, market)
        self.assertAlmostEqual(boosted, base)

    def test_should_alert_threshold(self):
        """Test alert threshold checking."""
        self.assertTrue(self.detector.should_alert(40.0))
        self.assertTrue(self.detector.should_alert(100.0))
        self.assertFalse(self.detector.should_alert(39.9))
        self.assertFalse(self.detector.should_alert(0.0))


class TestMarketMatcher(unittest.TestCase):
    """Test cross-platform market matching."""

    def setUp(self):
        self.matcher = MarketMatcher()

    def test_exact_name_match(self):
        """Test markets with identical names are paired."""
        pm = make_market(name="Will Trump win 2024?", market_id="pm-1")
        km = make_market(
            name="Will Trump win 2024?",
            platform=Platform.KALSHI,
            market_id="km-1",
        )

        pairs = self.matcher.find_pairs([pm], [km])
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0].polymarket.market_id, "pm-1")
        self.assertEqual(pairs[0].kalshi.market_id, "km-1")

    def test_fuzzy_match(self):
        """Test markets with similar names are paired."""
        pm = make_market(name="Trump wins presidential election 2024", market_id="pm-1")
        km = make_market(
            name="Trump wins 2024 presidential election",
            platform=Platform.KALSHI,
            market_id="km-1",
        )

        pairs = self.matcher.find_pairs([pm], [km])
        self.assertEqual(len(pairs), 1)

    def test_no_match_different_topics(self):
        """Test unrelated markets are not paired."""
        pm = make_market(name="Trump wins 2024 election", market_id="pm-1")
        km = make_market(
            name="Bitcoin exceeds 100000 dollars",
            platform=Platform.KALSHI,
            market_id="km-1",
        )

        pairs = self.matcher.find_pairs([pm], [km])
        self.assertEqual(len(pairs), 0)

    def test_no_duplicate_kalshi_matches(self):
        """Test that a Kalshi market is only matched once."""
        pm1 = make_market(name="Trump wins election 2024", market_id="pm-1")
        pm2 = make_market(name="Trump victory in 2024 election", market_id="pm-2")
        km = make_market(
            name="Trump wins 2024 election",
            platform=Platform.KALSHI,
            market_id="km-1",
        )

        pairs = self.matcher.find_pairs([pm1, pm2], [km])
        # Kalshi market should only appear in one pair
        kalshi_ids = [p.kalshi.market_id for p in pairs]
        self.assertEqual(len(kalshi_ids), len(set(kalshi_ids)))

    def test_normalize_name(self):
        """Test name normalization removes punctuation and collapses spaces."""
        result = self.matcher._normalize_name("  Will  Trump  win??? (2024)  ")
        self.assertEqual(result, "will trump win 2024")

    def test_names_match_high_overlap(self):
        """Test that high word overlap returns True."""
        self.assertTrue(self.matcher._names_match(
            "trump wins 2024 election",
            "trump wins 2024 presidential election",
        ))

    def test_names_match_low_overlap(self):
        """Test that low word overlap returns False."""
        self.assertFalse(self.matcher._names_match(
            "bitcoin price above 100000",
            "trump wins 2024 election",
        ))


if __name__ == "__main__":
    unittest.main()
