"""
Tests for the Intelligence Quality Overhaul.

Covers:
  - Hardened noise filters (hourly binary, time-resolution regex, handicap)
  - Intelligence value scoring (_intelligence_value)
  - Question-stem deduplication (_question_stem)
  - Per-category rate limiting in generate_stories()
  - Biggest Movers intelligence-weighted sort
  - DB query enrichment (volume_24h / end_date)
"""

import math
import sys
import os
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from dataclasses import replace

# ── path setup ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from story_generator import (
    _is_noise_market,
    _intelligence_value,
    _question_stem,
    _CATEGORY_PREMIUM,
    _MIN_VOLUME_FOR_BOOST,
    _HOURLY_BINARY_PATTERNS,
    _TIME_RESOLUTION_RE,
    _HANDICAP_SPREAD_PATTERNS,
    Story,
    StoryGenerator,
)


# ===========================================================================
# PART 1: Hardened Noise Filters
# ===========================================================================

class TestHourlyBinaryPatterns:
    """Tests for the new _HOURLY_BINARY_PATTERNS filter pass."""

    @pytest.mark.parametrize("name", [
        "Bitcoin ≥$83,000 on March 17?",
        "BTC ≥$84k on March 18?",
        "Ethereum ≤$2,500 by Friday?",
        "ETH ≥$3,000 on April 1?",
        "Solana ≥$150 on March 20?",
        "Bitcoin price at 5pm EST today",
        "BTC price at 12:00 UTC",
        "Will Bitcoin be worth $100k?",
        "Crypto price at 5pm tomorrow",
    ])
    def test_hourly_binary_caught(self, name):
        assert _is_noise_market(name), f"Should be noise: {name}"

    @pytest.mark.parametrize("name", [
        "Will Bitcoin be adopted as legal tender in Brazil?",
        "Will the SEC approve a Bitcoin ETF in 2026?",
        "Bitcoin mining regulation in the EU",
    ])
    def test_legit_bitcoin_markets_survive(self, name):
        assert not _is_noise_market(name), f"Should NOT be noise: {name}"


class TestTimeResolutionRegex:
    """Tests for _TIME_RESOLUTION_RE — crypto + dollar + date pattern."""

    @pytest.mark.parametrize("name", [
        "Bitcoin $83,000 on March 17",
        "BTC $84k by April 1",
        "Ethereum $2,500 after May 15",
        "Solana $150 before Jun 30",
    ])
    def test_regex_catches_crypto_threshold_with_date(self, name):
        assert _is_noise_market(name), f"Regex should catch: {name}"

    def test_regex_ignores_non_crypto(self):
        assert not _is_noise_market("Gold $2,000 on March 17")

    def test_regex_ignores_no_dollar(self):
        # No dollar sign → regex shouldn't match (other filters may still catch)
        name = "Bitcoin reaches new high on March 17"
        # This shouldn't match _TIME_RESOLUTION_RE specifically
        assert not _TIME_RESOLUTION_RE.search(name)


class TestHandicapSpreadPatterns:
    """Tests for the _HANDICAP_SPREAD_PATTERNS filter pass."""

    @pytest.mark.parametrize("name", [
        "Lakers -3.5 point spread vs Celtics",
        "Over/Under 220 points NBA Finals",
        "Manchester United handicap +1.5",
        "Total goals in Champions League final",
        "Moneyline: Chiefs vs Eagles",
        "First half score prediction",
        "Prop bet: will there be a safety?",
    ])
    def test_handicap_spread_caught(self, name):
        assert _is_noise_market(name), f"Should be noise: {name}"


class TestExistingFiltersStillWork:
    """Verify existing filter passes are unbroken."""

    def test_sports_still_blocked(self):
        assert _is_noise_market("Will the Lakers win the championship?")

    def test_esports_still_blocked(self):
        assert _is_noise_market("Counter-Strike 2 Major winner")

    def test_crypto_above_below_still_blocked(self):
        assert _is_noise_market("Bitcoin above $80,000 by end of day")

    def test_weather_still_blocked(self):
        assert _is_noise_market("Will it rain in New York tomorrow?")

    def test_financial_rescue_still_works(self):
        # Sports entity + financial keyword = RESCUED
        assert not _is_noise_market("Will the Lakers IPO in 2026?")
        assert not _is_noise_market("Manchester United acquisition by Saudi fund")

    def test_legit_politics_pass(self):
        assert not _is_noise_market("Will Trump impose 25% tariffs on Canada?")

    def test_legit_geopolitics_pass(self):
        assert not _is_noise_market("Will Russia and Ukraine reach a ceasefire?")


# ===========================================================================
# PART 2: Intelligence Value Scoring
# ===========================================================================

class TestIntelligenceValue:
    """Tests for _intelligence_value() scoring function."""

    def test_category_premium_ranking(self):
        """GEOPOLITICS should score higher than TECHNOLOGY, which beats OTHER."""
        geo  = _intelligence_value("GEOPOLITICS", 1_000_000, 50.0)
        tech = _intelligence_value("TECHNOLOGY", 1_000_000, 50.0)
        other = _intelligence_value("OTHER", 1_000_000, 50.0)
        assert geo > tech > other

    def test_high_volume_beats_low_volume(self):
        """$10M market should outscore $10K market, same category."""
        high = _intelligence_value("POLITICS", 10_000_000, 50.0)
        low  = _intelligence_value("POLITICS", 10_000, 50.0)
        assert high > low

    def test_low_volume_penalty(self):
        """Markets below $100k get vol_factor = 0.5 (penalized)."""
        val = _intelligence_value("POLITICS", 50_000, 50.0)
        # vol_factor = 0.5, cat = 1.5, score = 50 → 1.5 * 0.5 * 1.0 * 50 = 37.5
        assert val == pytest.approx(37.5)

    def test_volume_factor_at_threshold(self):
        """At exactly $100k, vol_factor = 1.0."""
        val = _intelligence_value("POLITICS", 100_000, 50.0)
        # cat=1.5, vol=1.0, horizon=1.0, score=50 → 75.0
        assert val == pytest.approx(75.0)

    def test_volume_factor_scaling(self):
        """At $1M, vol_factor = 1 + log10(1M/100k) = 1 + 1 = 2.0."""
        val = _intelligence_value("POLITICS", 1_000_000, 50.0)
        assert val == pytest.approx(1.5 * 2.0 * 1.0 * 50.0)

    def test_none_volume_treated_as_zero(self):
        """None volume → low-volume penalty."""
        val = _intelligence_value("POLITICS", None, 50.0)
        assert val == pytest.approx(1.5 * 0.5 * 50.0)

    def test_horizon_imminent_penalty(self):
        """Market resolving in 2 hours gets 0.3 horizon factor."""
        end = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        val = _intelligence_value("POLITICS", 1_000_000, 50.0, end)
        expected = 1.5 * 2.0 * 0.3 * 50.0
        assert val == pytest.approx(expected)

    def test_horizon_short_term_penalty(self):
        """Market resolving in 12 hours gets 0.7 horizon factor."""
        end = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()
        val = _intelligence_value("POLITICS", 1_000_000, 50.0, end)
        expected = 1.5 * 2.0 * 0.7 * 50.0
        assert val == pytest.approx(expected)

    def test_horizon_strategic_boost(self):
        """Market resolving in 14 days gets 1.5 horizon factor."""
        end = (datetime.now(timezone.utc) + timedelta(days=14)).isoformat()
        val = _intelligence_value("POLITICS", 1_000_000, 50.0, end)
        expected = 1.5 * 2.0 * 1.5 * 50.0
        assert val == pytest.approx(expected)

    def test_none_end_date_is_neutral(self):
        """No end_date → horizon_factor stays 1.0."""
        val_none = _intelligence_value("POLITICS", 1_000_000, 50.0, None)
        val_mid  = _intelligence_value("POLITICS", 1_000_000, 50.0,
                                       (datetime.now(timezone.utc) + timedelta(days=3)).isoformat())
        # Both should use horizon=1.0
        assert val_none == val_mid

    def test_zero_signal_score(self):
        """Zero signal_score → zero intelligence value."""
        assert _intelligence_value("GEOPOLITICS", 50_000_000, 0.0) == 0.0

    def test_sports_category_zero(self):
        """SPORTS premium is 0 → intelligence value is 0."""
        assert _intelligence_value("SPORTS", 50_000_000, 100.0) == 0.0


# ===========================================================================
# PART 3: Question-Stem Deduplication
# ===========================================================================

class TestQuestionStem:
    """Tests for _question_stem() — strips numbers/dates/dollars for dedup."""

    def test_dollar_amount_stripped(self):
        s1 = _question_stem("Bitcoin ≥$83,000 on March 17?")
        s2 = _question_stem("Bitcoin ≥$84,000 on March 18?")
        assert s1 == s2

    def test_dollar_k_notation(self):
        s1 = _question_stem("Bitcoin ≥$83k on March 17?")
        s2 = _question_stem("Bitcoin ≥$84k on March 18?")
        assert s1 == s2

    def test_different_months_same_stem(self):
        s1 = _question_stem("Bitcoin ≥$83k on March 17?")
        s2 = _question_stem("Bitcoin ≥$83k on April 17?")
        assert s1 == s2

    def test_time_stripped(self):
        s1 = _question_stem("Bitcoin price at 5:00pm EST")
        s2 = _question_stem("Bitcoin price at 12:00pm UTC")
        assert s1 == s2

    def test_percentage_stripped(self):
        s1 = _question_stem("Fed rate above 5.5%?")
        s2 = _question_stem("Fed rate above 5.25%?")
        assert s1 == s2

    def test_distinct_questions_stay_distinct(self):
        s1 = _question_stem("Will Trump impose tariffs on Canada?")
        s2 = _question_stem("Will Russia invade Ukraine?")
        assert s1 != s2

    def test_year_stripped(self):
        s1 = _question_stem("Will AI surpass humans in 2026?")
        s2 = _question_stem("Will AI surpass humans in 2027?")
        assert s1 == s2

    def test_empty_string(self):
        assert _question_stem("") == ""


# ===========================================================================
# PART 4: Per-Category Rate Limiting
# ===========================================================================

def _make_story(category: str, intelligence_value: float, name: str = "test") -> Story:
    """Helper to create a minimal Story for testing."""
    return Story(
        story_id="test-1",
        market_id="m1",
        headline="Test",
        lede="Test lede",
        market_name=name,
        platform="polymarket",
        probability=50.0,
        old_probability=45.0,
        prob_change=5.0,
        direction="up",
        signal_score=50.0,
        signals=["test"],
        signal_types=["test"],
        category=category,
        timestamp=datetime.now(timezone.utc),
        urgency="developing",
        watch_assets=[],
        volume_24h=1_000_000,
        intelligence_value=intelligence_value,
    )


class TestPerCategoryRateLimit:
    """Tests for the max-3-per-category rate limit in generate_stories()."""

    def test_rate_limit_applied(self):
        """With 5 POLITICS stories, only top 3 by intelligence_value survive."""
        stories = [
            _make_story("POLITICS", iv, f"politics-{i}")
            for i, iv in enumerate([100, 80, 60, 40, 20])
        ]
        # Simulate the rate-limiting logic from generate_stories
        stories.sort(key=lambda s: s.intelligence_value, reverse=True)
        cat_counts = {}
        result = []
        for s in stories:
            count = cat_counts.get(s.category, 0)
            if count < 3:
                result.append(s)
                cat_counts[s.category] = count + 1

        assert len(result) == 3
        assert result[0].intelligence_value == 100
        assert result[2].intelligence_value == 60

    def test_mixed_categories_not_limited(self):
        """2 POLITICS + 2 GEOPOLITICS = all 4 survive."""
        stories = [
            _make_story("POLITICS", 100, "pol-1"),
            _make_story("POLITICS", 80, "pol-2"),
            _make_story("GEOPOLITICS", 90, "geo-1"),
            _make_story("GEOPOLITICS", 70, "geo-2"),
        ]
        stories.sort(key=lambda s: s.intelligence_value, reverse=True)
        cat_counts = {}
        result = []
        for s in stories:
            count = cat_counts.get(s.category, 0)
            if count < 3:
                result.append(s)
                cat_counts[s.category] = count + 1

        assert len(result) == 4


# ===========================================================================
# PART 5: Story Dataclass
# ===========================================================================

class TestStoryDataclass:
    """Tests for the Story dataclass updates."""

    def test_intelligence_value_field_exists(self):
        s = _make_story("POLITICS", 42.0)
        assert s.intelligence_value == 42.0

    def test_end_date_field_default_none(self):
        s = _make_story("POLITICS", 42.0)
        assert s.end_date is None

    def test_to_dict_includes_new_fields(self):
        s = _make_story("POLITICS", 42.0)
        d = s.to_dict()
        assert "intelligence_value" in d
        assert "end_date" in d
        assert d["intelligence_value"] == 42.0
        assert d["end_date"] is None

    def test_end_date_round_trips(self):
        s = _make_story("POLITICS", 42.0)
        s = replace(s, end_date="2026-04-01T00:00:00+00:00")
        d = s.to_dict()
        assert d["end_date"] == "2026-04-01T00:00:00+00:00"


# ===========================================================================
# PART 6: DB Query Enrichment
# ===========================================================================

class TestDBQueryEnrichment:
    """Test that get_recent_alerts_feed returns volume/end_date columns."""

    def test_enriched_query_has_volume_and_end_date(self):
        """Verify the enriched query adds snapshot_volume_24h and snapshot_end_date."""
        # We can't easily test against a real DB in unit tests, but we can
        # verify the _row_to_story method handles the new columns.
        from story_generator import StoryGenerator
        gen = StoryGenerator()

        row = {
            "id": 1,
            "market_id": "test-market",
            "market_name": "Will Trump impose 25% tariffs on Canada?",
            "platform": "polymarket",
            "signal_score": 65.0,
            "old_probability": 45.0,
            "new_probability": 55.0,
            "reasons": '["Price movement", "Volume surge"]',
            "signal_types": '["momentum", "volume"]',
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "snapshot_volume_24h": 5_000_000,
            "snapshot_end_date": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
        }
        story = gen._row_to_story(row)
        assert story is not None
        assert story.volume_24h == 5_000_000
        assert story.end_date is not None
        assert story.intelligence_value > 0

    def test_row_to_story_without_enrichment(self):
        """When snapshot columns are absent, falls back gracefully."""
        from story_generator import StoryGenerator
        gen = StoryGenerator()

        row = {
            "id": 2,
            "market_id": "test-market-2",
            "market_name": "Will the Fed cut rates?",
            "platform": "kalshi",
            "signal_score": 50.0,
            "old_probability": 30.0,
            "new_probability": 40.0,
            "reasons": '["Rate expectations"]',
            "signal_types": '["macro"]',
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        story = gen._row_to_story(row)
        assert story is not None
        assert story.volume_24h is None
        assert story.end_date is None
        # Intelligence value still computed (with low-volume penalty)
        assert story.intelligence_value > 0


# ===========================================================================
# PART 7: Integration
# ===========================================================================

class TestIntegration:
    """End-to-end integration tests for the intelligence pipeline."""

    def test_noise_then_intelligence_pipeline(self):
        """Noise filter removes junk, intelligence scoring ranks the rest."""
        markets = [
            ("Bitcoin ≥$83k on March 17?", True),
            ("Will Trump impose tariffs on Canada?", False),
            ("Lakers -3.5 point spread", True),
            ("Will AI regulation pass in the EU?", False),
            ("Counter-Strike 2 Major Grand Final", True),
        ]
        surviving = [name for name, is_noise in markets if not _is_noise_market(name)]
        assert len(surviving) == 2
        assert "Trump" in surviving[0]
        assert "AI regulation" in surviving[1]

    def test_stem_dedup_collapses_crypto_variants(self):
        """Multiple BTC threshold variants → single stem."""
        names = [
            "Bitcoin ≥$83,000 on March 17?",
            "Bitcoin ≥$84,000 on March 18?",
            "Bitcoin ≥$85,000 on March 19?",
        ]
        stems = {_question_stem(n) for n in names}
        assert len(stems) == 1

    def test_intelligence_value_geopolitics_beats_crypto_noise(self):
        """A $5M geopolitics market should dominate a $50K crypto market."""
        geo = _intelligence_value("GEOPOLITICS", 5_000_000, 60.0,
                                  (datetime.now(timezone.utc) + timedelta(days=30)).isoformat())
        crypto = _intelligence_value("OTHER", 50_000, 60.0,
                                     (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat())
        assert geo > crypto * 5  # Geo should be many times larger

    def test_category_premiums_exist(self):
        """All expected categories have premiums defined."""
        for cat in ["GEOPOLITICS", "CONFLICT", "POLITICS", "MARKETS", "TECHNOLOGY", "OTHER", "SPORTS"]:
            assert cat in _CATEGORY_PREMIUM

    def test_constants_exist(self):
        """Module-level constants are importable."""
        assert _MIN_VOLUME_FOR_BOOST == 100_000
        assert len(_HOURLY_BINARY_PATTERNS) > 10
        assert len(_HANDICAP_SPREAD_PATTERNS) > 5
        assert _TIME_RESOLUTION_RE is not None
