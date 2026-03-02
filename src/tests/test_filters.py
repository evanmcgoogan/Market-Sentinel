"""Tests for market filtering logic."""

import os
import sys
import unittest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models import Market, Platform
from config import MarketFilterConfig
from filters import MarketFilter


def make_market(name, category="", tags=None, description="") -> Market:
    """Helper to create test markets."""
    return Market(
        platform=Platform.POLYMARKET,
        market_id="test",
        slug="test",
        name=name,
        description=description,
        category=category,
        tags=tags or [],
    )


class TestMarketFilter(unittest.TestCase):
    """Test market filtering."""

    def setUp(self):
        self.filter = MarketFilter(MarketFilterConfig())

    def test_includes_political_market(self):
        """Test that political markets are included."""
        market = make_market("Will Trump win the 2024 election?")
        self.assertTrue(self.filter.should_monitor(market))

    def test_includes_geopolitical_market(self):
        """Test that geopolitical markets are included."""
        market = make_market("Will China invade Taiwan by 2026?")
        self.assertTrue(self.filter.should_monitor(market))

    def test_includes_ai_market(self):
        """Test that AI-related markets are included."""
        market = make_market("Will OpenAI release GPT-5 in 2025?")
        self.assertTrue(self.filter.should_monitor(market))

    def test_includes_economic_market(self):
        """Test that economic markets are included."""
        market = make_market("Will the Fed cut interest rates in March?")
        self.assertTrue(self.filter.should_monitor(market))

    def test_excludes_sports_market(self):
        """Test that sports markets are excluded."""
        market = make_market("Will the NBA finals go to 7 games?")
        self.assertFalse(self.filter.should_monitor(market))

    def test_excludes_entertainment_market(self):
        """Test that entertainment markets are excluded."""
        market = make_market("Will the new Netflix series win an Emmy?")
        self.assertFalse(self.filter.should_monitor(market))

    def test_excludes_celebrity_market(self):
        """Test that celebrity markets are excluded."""
        market = make_market("Will Kardashian launch new brand?")
        self.assertFalse(self.filter.should_monitor(market))

    def test_exclude_takes_priority(self):
        """Test that exclusion keywords override inclusion."""
        # "Super Bowl" contains exclusion keywords even though
        # it might match something political in theory
        market = make_market("Super Bowl MVP prediction")
        self.assertFalse(self.filter.should_monitor(market))

    def test_no_match_excluded(self):
        """Test that unrecognized markets are excluded by default."""
        market = make_market("Will aliens contact Earth by 2030?")
        self.assertFalse(self.filter.should_monitor(market))

    def test_category_inclusion(self):
        """Test that matching category includes market."""
        market = make_market("Some market", category="politics")
        self.assertTrue(self.filter.should_monitor(market))

    def test_category_exclusion(self):
        """Test that excluded category removes market."""
        market = make_market("Some sports bet", category="sports")
        self.assertFalse(self.filter.should_monitor(market))

    def test_filter_markets_batch(self):
        """Test batch filtering."""
        markets = [
            make_market("Trump election 2024"),
            make_market("NBA finals game 7"),
            make_market("Fed interest rate decision"),
            make_market("Netflix subscriber count"),
        ]

        filtered = self.filter.filter_markets(markets)
        names = [m.name for m in filtered]

        self.assertIn("Trump election 2024", names)
        self.assertIn("Fed interest rate decision", names)
        self.assertNotIn("NBA finals game 7", names)
        self.assertNotIn("Netflix subscriber count", names)

    def test_get_match_reason(self):
        """Test that match reason is returned."""
        market = make_market("Will Trump win the election?")
        reason = self.filter.get_match_reason(market)
        self.assertIn("Keyword", reason)


class TestMarketFilterSearchableText(unittest.TestCase):
    """Test that all text fields are searched."""

    def setUp(self):
        self.filter = MarketFilter(MarketFilterConfig())

    def test_matches_description(self):
        """Test that description text is searched."""
        market = make_market(
            "Generic question?",
            description="This market tracks the election outcome",
        )
        self.assertTrue(self.filter.should_monitor(market))

    def test_matches_tags(self):
        """Test that tag text is searched."""
        market = make_market("Some question?", tags=["politics", "us-2024"])
        self.assertTrue(self.filter.should_monitor(market))


if __name__ == "__main__":
    unittest.main()
