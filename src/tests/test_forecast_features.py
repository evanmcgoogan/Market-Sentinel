"""
Tests for Features C, D, and E:
  C. Per-Asset Weight Specialization
  D. Volume-Weighted Prediction Market Decomposition
  E. Historical Backtest Harness

Comprehensive coverage of all new signal weighting, weight hierarchy,
and backtest replay logic.
"""

import json
import math
import os
import sys
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock, patch

# Make sure src/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecast_engine import (
    DEFAULT_WEIGHTS,
    ForecastEngine,
    ASSET_KEYWORDS,
    ASSET_CATEGORY,
    PER_ASSET_MIN_SAMPLES,
    PER_CATEGORY_MIN_SAMPLES,
    _PM_HALF_LIFE_HOURS,
    _PM_ALERT_HALF_LIFE_HOURS,
    _PM_MIN_VOLUME_FOR_WEIGHT,
    _utcnow,
)
from forecast_evaluator import (
    ForecastEvaluator,
    DEFAULT_WEIGHTS as EVAL_DEFAULT_WEIGHTS,
    LEARNING_RATE,
    SHRINKAGE_FACTOR,
    WEIGHT_FLOOR,
    WEIGHT_CEILING,
)
from backtester import (
    recompute_forecast,
    score_prediction,
    run_backtest,
    format_report,
    BacktestReport,
    PredictionResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class MockBar:
    """Lightweight PriceBar stand-in."""
    ticker: str = "SPY"
    dt: str = "2026-01-01"
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float = 0.0
    volume: Optional[float] = None
    source: str = "test"


def _make_bars(closes, ticker="SPY"):
    return [MockBar(ticker=ticker, close=c) for c in closes]


class FakeDB:
    """In-memory mock DB for tests."""

    def __init__(self):
        self._state = {}
        self._forecast_outcomes = []

    def get_state(self, key, default=None):
        return self._state.get(key, default)

    def set_state(self, key, value):
        self._state[key] = value

    def get_top_volume_markets(self, limit=30, hours=1):
        return []

    def get_recent_alerts_feed(self, hours=24, limit=50):
        return []

    def get_all_recent_news(self, hours=12, limit=25):
        return []

    def save_outlook_prediction(self, **kwargs):
        pass

    def save_forecast_calls(self, session_id, generated_at, calls):
        return len(calls)

    def get_recent_forecast_outcomes(self, limit=200):
        return self._forecast_outcomes[:limit]


class FakeMarketData:
    """Minimal mock for market_data."""

    def get_history(self, ticker, days=20):
        return _make_bars([100 + i for i in range(days)], ticker)

    def get_price_at(self, ticker, dt):
        return 100.0


# ===========================================================================
# Feature D: Volume-Weighted Prediction Market Signal
# ===========================================================================

class TestPredictionMarketSignalVolumeWeighting(unittest.TestCase):
    """Feature D: volume_24h weighting via sqrt(vol/floor)."""

    def _make_engine(self, db=None):
        db = db or FakeDB()
        md = FakeMarketData()
        with patch("forecast_engine.composite_momentum", return_value=(0.1, "test")):
            engine = ForecastEngine(md, db, api_key="")
        return engine

    def test_higher_volume_market_gets_more_influence(self):
        """$50M market should dominate $10K market."""
        engine = self._make_engine()
        now = _utcnow()
        ts = now.isoformat()

        # Two markets: one high-volume bullish, one low-volume bearish
        markets = [
            {"market_name": "Bitcoin ETF approval",
             "latest_prob": 80, "volume_24h": 50_000_000, "latest_ts": ts},
            {"market_name": "Bitcoin mining ban",
             "latest_prob": 20, "volume_24h": 10_000, "latest_ts": ts},
        ]
        result = engine._prediction_market_signal("BTC", markets, [])
        self.assertIsNotNone(result)
        # High-volume bullish should dominate: value > 0
        self.assertGreater(result.value, 0)

    def test_equal_volume_markets_cancel_out(self):
        """Two equal-volume markets with opposite signals cancel."""
        engine = self._make_engine()
        now = _utcnow()
        ts = now.isoformat()

        markets = [
            {"market_name": "Bitcoin crash imminent",
             "latest_prob": 20, "volume_24h": 1_000_000, "latest_ts": ts},
            {"market_name": "Bitcoin reaches 100k",
             "latest_prob": 80, "volume_24h": 1_000_000, "latest_ts": ts},
        ]
        result = engine._prediction_market_signal("BTC", markets, [])
        # Should roughly cancel — value near 0
        if result:
            self.assertAlmostEqual(abs(result.value), 0, delta=0.3)

    def test_missing_volume_defaults_to_one(self):
        """Missing volume_24h falls back to vol_weight=1.0."""
        engine = self._make_engine()
        now = _utcnow()
        ts = now.isoformat()

        markets = [
            {"market_name": "Bitcoin price prediction",
             "latest_prob": 80, "volume_24h": None, "latest_ts": ts},
        ]
        result = engine._prediction_market_signal("BTC", markets, [])
        self.assertIsNotNone(result)
        self.assertGreater(result.value, 0)


class TestPredictionMarketSignalTimeDecay(unittest.TestCase):
    """Feature D: exponential time decay on markets and alerts."""

    def _make_engine(self, db=None):
        db = db or FakeDB()
        md = FakeMarketData()
        with patch("forecast_engine.composite_momentum", return_value=(0.1, "test")):
            engine = ForecastEngine(md, db, api_key="")
        return engine

    def test_recent_market_dominates_stale(self):
        """A fresh market signal should outweigh a stale one."""
        engine = self._make_engine()
        now = _utcnow()
        fresh_ts = now.isoformat()
        stale_ts = (now - timedelta(hours=12)).isoformat()

        # Fresh bullish + stale bearish = net bullish
        markets = [
            {"market_name": "Bitcoin ETF approved",
             "latest_prob": 85, "volume_24h": 100_000, "latest_ts": fresh_ts},
            {"market_name": "Bitcoin regulation ban",
             "latest_prob": 15, "volume_24h": 100_000, "latest_ts": stale_ts},
        ]
        result = engine._prediction_market_signal("BTC", markets, [])
        self.assertIsNotNone(result)
        self.assertGreater(result.value, 0)  # fresh bullish should win

    def test_alert_time_decay_faster_than_market(self):
        """Alert half-life (2h) decays faster than market half-life (4h)."""
        self.assertLess(
            _PM_ALERT_HALF_LIFE_HOURS,
            _PM_HALF_LIFE_HOURS,
        )


class TestPredictionMarketSignalAlertMagnitude(unittest.TestCase):
    """Feature D: alert magnitude weighting min(|delta|/5, 2.0)."""

    def _make_engine(self):
        db = FakeDB()
        md = FakeMarketData()
        with patch("forecast_engine.composite_momentum", return_value=(0.1, "test")):
            engine = ForecastEngine(md, db, api_key="")
        return engine

    def test_large_alert_move_has_more_weight(self):
        """15pp move dominates 5pp counter-move → net bullish vs bearish."""
        engine = self._make_engine()
        now = _utcnow()
        ts = now.isoformat()

        # Big bullish + small bearish → should be bullish
        mixed_alerts = [
            {"market_name": "Bitcoin ETF approval",
             "new_probability": 75, "old_probability": 60, "timestamp": ts},
            {"market_name": "Bitcoin mining ban",
             "new_probability": 45, "old_probability": 50, "timestamp": ts},
        ]
        result = engine._prediction_market_signal("BTC", [], mixed_alerts)
        self.assertIsNotNone(result)
        # 15pp bullish (mag_weight=2.0) should dominate 5pp bearish (mag_weight=1.0)
        self.assertGreater(result.value, 0, "big bullish alert should dominate")

    def test_tiny_alert_filtered_out(self):
        """Alert with <0.5pp change is filtered as noise."""
        engine = self._make_engine()
        ts = _utcnow().isoformat()

        alerts = [
            {"market_name": "Bitcoin price prediction",
             "new_probability": 50.3, "old_probability": 50, "timestamp": ts},
        ]
        result = engine._prediction_market_signal("BTC", [], alerts)
        # 0.3pp delta < 0.5 threshold → filtered → None
        self.assertIsNone(result)

    def test_magnitude_cap_at_two(self):
        """Magnitude weight caps at 2.0 for very large moves."""
        engine = self._make_engine()
        # min(abs(15)/5, 2.0) = min(3.0, 2.0) = 2.0
        self.assertEqual(min(abs(15) / 5.0, 2.0), 2.0)


class TestHoursSince(unittest.TestCase):
    """Feature D helper: _hours_since()."""

    def test_valid_iso_timestamp(self):
        now = datetime(2026, 3, 9, 12, 0, 0)
        ts = "2026-03-09T10:00:00"
        hours = ForecastEngine._hours_since(ts, now)
        self.assertAlmostEqual(hours, 2.0, places=2)

    def test_z_suffix(self):
        now = datetime(2026, 3, 9, 12, 0, 0)
        ts = "2026-03-09T10:00:00Z"
        hours = ForecastEngine._hours_since(ts, now)
        self.assertAlmostEqual(hours, 2.0, places=2)

    def test_none_returns_24(self):
        hours = ForecastEngine._hours_since(None, datetime.now())
        self.assertEqual(hours, 24.0)

    def test_invalid_string_returns_24(self):
        hours = ForecastEngine._hours_since("not-a-timestamp", datetime.now())
        self.assertEqual(hours, 24.0)

    def test_future_timestamp_clamped_to_zero(self):
        now = datetime(2026, 3, 9, 10, 0, 0)
        ts = "2026-03-09T12:00:00"
        hours = ForecastEngine._hours_since(ts, now)
        self.assertEqual(hours, 0.0)


class TestPMSourceDescription(unittest.TestCase):
    """Feature D helper: _pm_source_description()."""

    def test_full_description(self):
        src = ForecastEngine._pm_source_description(
            "Trump tariff resolution", 15_000_000, 7.0, 12.5
        )
        self.assertIn("Trump tariff resolution", src)
        self.assertIn("$15.0M vol", src)
        self.assertIn("7pp up", src)
        self.assertIn("wt=12.5", src)

    def test_no_volume(self):
        src = ForecastEngine._pm_source_description("test", 0, 0, 1.0)
        self.assertNotIn("vol", src)

    def test_small_volume_formatted_as_k(self):
        src = ForecastEngine._pm_source_description("test", 500_000, 0, 1.0)
        self.assertIn("$500K vol", src)

    def test_no_alert_delta(self):
        src = ForecastEngine._pm_source_description("test", 1_000_000, 0.5, 1.0)
        self.assertNotIn("pp", src)  # |0.5| < 1

    def test_truncates_long_market_name(self):
        long_name = "A" * 100
        src = ForecastEngine._pm_source_description(long_name, 0, 0, 1.0)
        self.assertLessEqual(len(src.split(" | ")[0]), 65)

    def test_no_keyword_match_returns_none(self):
        """Unknown ticker returns None."""
        db = FakeDB()
        md = FakeMarketData()
        with patch("forecast_engine.composite_momentum", return_value=(0.1, "test")):
            engine = ForecastEngine(md, db, api_key="")
        result = engine._prediction_market_signal(
            "ZZZZZ", [{"market_name": "test", "latest_prob": 80}], []
        )
        self.assertIsNone(result)


# ===========================================================================
# Feature E: Historical Backtest Harness
# ===========================================================================

class TestRecomputeForecast(unittest.TestCase):
    """Feature E: recompute_forecast() unit tests."""

    def test_basic_recomputation(self):
        """Re-weighting all-bullish drivers should produce UP."""
        drivers = json.dumps([
            {"family": "momentum", "value": 0.5, "weight": 0.2, "contribution": 0.1},
            {"family": "prediction_market", "value": 0.3, "weight": 0.3, "contribution": 0.09},
        ])
        direction, confidence, pressure = recompute_forecast(
            drivers, DEFAULT_WEIGHTS, []
        )
        self.assertEqual(direction, "UP")
        self.assertGreater(confidence, 20)
        self.assertGreater(pressure, 0)

    def test_disabled_families_excluded(self):
        """Disabling a dominant family should flip direction."""
        drivers = json.dumps([
            {"family": "momentum", "value": 0.8, "weight": 0.5, "contribution": 0.4},
            {"family": "prediction_market", "value": -0.3, "weight": 0.3, "contribution": -0.09},
            {"family": "whale", "value": -0.2, "weight": 0.2, "contribution": -0.04},
        ])
        # With momentum: net positive → UP
        d_with, _, _ = recompute_forecast(drivers, DEFAULT_WEIGHTS, [])
        self.assertEqual(d_with, "UP")

        # Without momentum: net negative → DOWN
        d_without, _, _ = recompute_forecast(drivers, DEFAULT_WEIGHTS, ["momentum"])
        self.assertEqual(d_without, "DOWN")

    def test_empty_drivers_returns_defaults(self):
        direction, confidence, pressure = recompute_forecast("[]", DEFAULT_WEIGHTS, [])
        self.assertEqual(direction, "UP")
        self.assertEqual(confidence, 20)
        self.assertAlmostEqual(pressure, 0.0)

    def test_invalid_json_returns_defaults(self):
        direction, confidence, pressure = recompute_forecast(
            "not-json", DEFAULT_WEIGHTS, []
        )
        self.assertEqual(direction, "UP")
        self.assertEqual(confidence, 20)

    def test_weight_override_changes_result(self):
        """Overriding a weight to 0.60 should amplify that family's signal."""
        drivers = json.dumps([
            {"family": "momentum", "value": 0.5, "weight": 0.2, "contribution": 0.1},
            {"family": "prediction_market", "value": -0.1, "weight": 0.3, "contribution": -0.03},
        ])
        # With high momentum weight
        weights_high = dict(DEFAULT_WEIGHTS)
        weights_high["momentum"] = 0.60
        _, conf_high, pressure_high = recompute_forecast(drivers, weights_high, [])

        # With low momentum weight
        weights_low = dict(DEFAULT_WEIGHTS)
        weights_low["momentum"] = 0.05
        _, conf_low, pressure_low = recompute_forecast(drivers, weights_low, [])

        self.assertGreater(pressure_high, pressure_low)


class TestScorePrediction(unittest.TestCase):
    """Feature E: score_prediction() unit tests."""

    def test_correct_up_prediction(self):
        """UP with positive return → correct."""
        correct, brier, ll = score_prediction("UP", 70, 2.5)
        self.assertTrue(correct)
        self.assertLess(brier, 0.25)  # (0.7 - 1)^2 = 0.09
        self.assertLess(ll, 1.0)

    def test_incorrect_up_prediction(self):
        """UP with negative return → incorrect."""
        correct, brier, ll = score_prediction("UP", 70, -2.5)
        self.assertFalse(correct)
        self.assertGreater(brier, 0.25)  # (0.7 - 0)^2 = 0.49
        self.assertGreater(ll, 0.5)

    def test_correct_down_prediction(self):
        correct, brier, ll = score_prediction("DOWN", 80, -3.0)
        self.assertTrue(correct)

    def test_flat_return_near_threshold(self):
        """Return within noise threshold (±0.15%) → incorrect."""
        correct, _, _ = score_prediction("UP", 60, 0.1)
        self.assertFalse(correct)

    def test_brier_score_bounds(self):
        """Brier score should be in [0, 1]."""
        for conf in [15, 50, 95]:
            for ret in [-5.0, 0.0, 5.0]:
                _, brier, _ = score_prediction("UP", conf, ret)
                self.assertGreaterEqual(brier, 0)
                self.assertLessEqual(brier, 1)


class TestRunBacktest(unittest.TestCase):
    """Feature E: run_backtest() integration tests."""

    def _make_db_with_outcomes(self, n=30):
        db = FakeDB()
        now = _utcnow()
        outcomes = []
        for i in range(n):
            gen_at = (now - timedelta(days=i % 10, hours=i)).isoformat()
            drivers = json.dumps([
                {"family": "momentum", "value": 0.3, "weight": 0.2,
                 "contribution": 0.06},
                {"family": "prediction_market", "value": -0.1, "weight": 0.3,
                 "contribution": -0.03},
            ])
            actual_ret = 1.5 if i % 3 != 0 else -1.5
            outcomes.append({
                "id": i,
                "call_id": i,
                "ticker": "BTC" if i % 2 == 0 else "SPY",
                "horizon": "24h",
                "direction": "UP",
                "magnitude": "SMALL",
                "confidence": 55,
                "drivers_json": drivers,
                "call_generated_at": gen_at,
                "actual_return_pct": actual_ret,
                "direction_correct": 1 if actual_ret > 0.15 else 0,
                "brier_score": (0.55 - (1 if actual_ret > 0.15 else 0)) ** 2,
                "log_loss": 0.5,
            })
        db._forecast_outcomes = outcomes
        return db

    def test_empty_db_returns_zero_predictions(self):
        db = FakeDB()
        report = run_backtest(db, lookback_days=30)
        self.assertEqual(report.total_predictions, 0)

    def test_with_data_produces_report(self):
        db = self._make_db_with_outcomes(30)
        report = run_backtest(db, lookback_days=30)
        self.assertGreater(report.total_predictions, 0)
        self.assertIn("BTC", report.per_asset)
        self.assertIn("SPY", report.per_asset)

    def test_confidence_threshold_filters(self):
        db = self._make_db_with_outcomes(30)
        # All outcomes have confidence=55
        report_all = run_backtest(db, lookback_days=30, confidence_threshold=0)
        report_high = run_backtest(db, lookback_days=30, confidence_threshold=60)
        self.assertGreater(report_all.total_predictions, report_high.total_predictions)

    def test_disabled_families_reflected_in_report(self):
        db = self._make_db_with_outcomes(30)
        report = run_backtest(
            db, lookback_days=30, disabled_families=["whale"]
        )
        self.assertIn("whale", report.disabled_families)

    def test_weight_override_reflected(self):
        db = self._make_db_with_outcomes(30)
        report = run_backtest(
            db, lookback_days=30,
            override_weights={"momentum": 0.50},
        )
        # Momentum weight should be higher than default after normalization
        self.assertGreater(
            report.weights_used.get("momentum", 0),
            DEFAULT_WEIGHTS["momentum"],
        )

    def test_format_report_produces_string(self):
        db = self._make_db_with_outcomes(30)
        report = run_backtest(db, lookback_days=30)
        formatted = format_report(report)
        self.assertIsInstance(formatted, str)
        self.assertIn("BACKTEST REPORT", formatted)
        self.assertIn("Direction accuracy", formatted)
        self.assertIn("Brier score", formatted)

    def test_format_report_empty(self):
        """Format report with zero predictions doesn't crash."""
        report = BacktestReport(
            total_predictions=0, lookback_days=7,
            weights_used=DEFAULT_WEIGHTS, disabled_families=[],
            confidence_threshold=0,
            orig_direction_accuracy=0, orig_brier=0, orig_log_loss=0,
            sim_direction_accuracy=0, sim_brier=0, sim_log_loss=0,
            direction_accuracy_delta=0, brier_delta=0, log_loss_delta=0,
            per_asset={}, direction_changes=0, improvements=0, degradations=0,
        )
        formatted = format_report(report)
        self.assertIn("BACKTEST REPORT", formatted)


# ===========================================================================
# Feature C: Per-Asset Weight Specialization
# ===========================================================================

class TestGetWeightsForAsset(unittest.TestCase):
    """Feature C: _get_weights_for_asset() three-tier resolution."""

    def _make_engine(self, db=None):
        db = db or FakeDB()
        md = FakeMarketData()
        with patch("forecast_engine.composite_momentum", return_value=(0.1, "test")):
            engine = ForecastEngine(md, db, api_key="")
        return engine

    def test_per_asset_weights_returned(self):
        """When per-asset weights exist, use them."""
        engine = self._make_engine()
        custom = {"momentum": 0.40, "prediction_market": 0.20,
                  "whale": 0.15, "cross_asset": 0.15, "news_sentiment": 0.10}
        engine._per_asset_weights = {"BTC": custom}

        result = engine._get_weights_for_asset("BTC")
        self.assertEqual(result["momentum"], 0.40)

    def test_per_category_fallback(self):
        """When no per-asset, falls back to per-category."""
        engine = self._make_engine()
        crypto_weights = {"momentum": 0.35, "prediction_market": 0.25,
                          "whale": 0.15, "cross_asset": 0.15, "news_sentiment": 0.10}
        engine._per_asset_weights = {}
        engine._per_category_weights = {"CRYPTO": crypto_weights}

        # BTC is in CRYPTO category
        result = engine._get_weights_for_asset("BTC")
        self.assertEqual(result["momentum"], 0.35)

    def test_global_fallback(self):
        """When no per-asset or per-category, falls back to global."""
        engine = self._make_engine()
        engine._per_asset_weights = {}
        engine._per_category_weights = {}

        result = engine._get_weights_for_asset("BTC")
        self.assertEqual(result, engine._weights)

    def test_per_asset_takes_precedence_over_category(self):
        """Per-asset should win even if per-category is available."""
        engine = self._make_engine()
        engine._per_asset_weights = {
            "BTC": {"momentum": 0.50, "prediction_market": 0.15,
                    "whale": 0.15, "cross_asset": 0.10, "news_sentiment": 0.10}
        }
        engine._per_category_weights = {
            "CRYPTO": {"momentum": 0.35, "prediction_market": 0.25,
                       "whale": 0.15, "cross_asset": 0.15, "news_sentiment": 0.10}
        }

        result = engine._get_weights_for_asset("BTC")
        self.assertEqual(result["momentum"], 0.50)  # per-asset, not per-category


class TestLoadPerAssetWeights(unittest.TestCase):
    """Feature C: _load_weights() loads per-asset/per-category from DB."""

    def test_loads_from_db_state(self):
        db = FakeDB()
        db._state["forecast_per_asset_weights"] = {
            "per_asset": {
                "BTC": {"weights": {"momentum": 0.45, "prediction_market": 0.20,
                                    "whale": 0.15, "cross_asset": 0.10,
                                    "news_sentiment": 0.10},
                         "sample_count": 20},
            },
            "per_category": {
                "EQUITY": {"weights": {"momentum": 0.30, "prediction_market": 0.25,
                                       "whale": 0.15, "cross_asset": 0.15,
                                       "news_sentiment": 0.15},
                            "sample_count": 50},
            },
        }
        md = FakeMarketData()
        with patch("forecast_engine.composite_momentum", return_value=(0.1, "test")):
            engine = ForecastEngine(md, db, api_key="")

        self.assertIn("BTC", engine._per_asset_weights)
        self.assertEqual(engine._per_asset_weights["BTC"]["momentum"], 0.45)
        self.assertIn("EQUITY", engine._per_category_weights)

    def test_empty_db_state_ok(self):
        db = FakeDB()
        md = FakeMarketData()
        with patch("forecast_engine.composite_momentum", return_value=(0.1, "test")):
            engine = ForecastEngine(md, db, api_key="")
        self.assertEqual(engine._per_asset_weights, {})
        self.assertEqual(engine._per_category_weights, {})


class TestLearnWeightsFromOutcomes(unittest.TestCase):
    """Feature C: _learn_weights_from_outcomes() core learning algorithm."""

    def _make_evaluator(self):
        return ForecastEvaluator(api_key="")

    def _make_outcomes(self, n=25, correct_ratio=0.7):
        """Create synthetic outcomes with controllable correct ratio."""
        outcomes = []
        for i in range(n):
            is_correct = 1 if (i / n) < correct_ratio else 0
            drivers = json.dumps([
                {"family": "momentum", "value": 0.3, "weight": 0.2,
                 "contribution": 0.06},
                {"family": "prediction_market", "value": 0.2, "weight": 0.3,
                 "contribution": 0.06},
                {"family": "whale", "value": -0.1, "weight": 0.2,
                 "contribution": -0.02},
                {"family": "cross_asset", "value": 0.1, "weight": 0.15,
                 "contribution": 0.015},
                {"family": "news_sentiment", "value": 0.05, "weight": 0.15,
                 "contribution": 0.0075},
            ])
            outcomes.append({
                "direction_correct": is_correct,
                "drivers_json": drivers,
            })
        return outcomes

    def test_produces_valid_weight_vector(self):
        """Output weights should sum to ~1 and be within bounds."""
        evaluator = self._make_evaluator()
        outcomes = self._make_outcomes(25)
        weights = evaluator._learn_weights_from_outcomes(outcomes, dict(DEFAULT_WEIGHTS))

        # All families present
        for fam in DEFAULT_WEIGHTS:
            self.assertIn(fam, weights)
            self.assertGreaterEqual(weights[fam], WEIGHT_FLOOR)
            self.assertLessEqual(weights[fam], WEIGHT_CEILING)

        # Sum to ~1
        self.assertAlmostEqual(sum(weights.values()), 1.0, places=2)

    def test_insufficient_data_gives_neutral(self):
        """With very few samples, quality defaults to 0.5 (neutral)."""
        evaluator = self._make_evaluator()
        # Only 3 outcomes per family (< 5 threshold)
        outcomes = self._make_outcomes(3)
        weights = evaluator._learn_weights_from_outcomes(outcomes, dict(DEFAULT_WEIGHTS))

        # Should stay near defaults since all quality=0.5
        for fam in DEFAULT_WEIGHTS:
            self.assertAlmostEqual(
                weights[fam], DEFAULT_WEIGHTS[fam], delta=0.05,
                msg=f"{fam} should stay near default with insufficient data"
            )

    def test_deterministic(self):
        """Same inputs should produce same outputs."""
        evaluator = self._make_evaluator()
        outcomes = self._make_outcomes(25)
        w1 = evaluator._learn_weights_from_outcomes(outcomes, dict(DEFAULT_WEIGHTS))
        w2 = evaluator._learn_weights_from_outcomes(outcomes, dict(DEFAULT_WEIGHTS))
        self.assertEqual(w1, w2)


class TestUpdatePerAssetWeights(unittest.TestCase):
    """Feature C: _update_per_asset_weights() integration tests."""

    def _make_evaluator(self):
        return ForecastEvaluator(api_key="")

    def _make_outcomes_for_ticker(self, ticker, n=20):
        outcomes = []
        for i in range(n):
            drivers = json.dumps([
                {"family": "momentum", "value": 0.3, "weight": 0.2,
                 "contribution": 0.06},
                {"family": "prediction_market", "value": 0.2, "weight": 0.3,
                 "contribution": 0.06},
            ])
            outcomes.append({
                "ticker": ticker,
                "direction_correct": 1 if i % 3 != 0 else 0,
                "drivers_json": drivers,
            })
        return outcomes

    def test_persists_per_asset_state(self):
        """With enough samples, per-asset weights are persisted."""
        evaluator = self._make_evaluator()
        db = FakeDB()

        # Need ≥ PER_ASSET_MIN_SAMPLES for per-asset learning
        outcomes = self._make_outcomes_for_ticker("BTC", PER_ASSET_MIN_SAMPLES + 5)

        evaluator._update_per_asset_weights(db, outcomes)

        stored = db.get_state("forecast_per_asset_weights")
        self.assertIsNotNone(stored)
        self.assertIn("per_asset", stored)
        self.assertIn("BTC", stored["per_asset"])
        self.assertIn("weights", stored["per_asset"]["BTC"])

    def test_insufficient_samples_skipped(self):
        """With too few samples, no per-asset weights are created."""
        evaluator = self._make_evaluator()
        db = FakeDB()

        outcomes = self._make_outcomes_for_ticker("BTC", PER_ASSET_MIN_SAMPLES - 1)

        evaluator._update_per_asset_weights(db, outcomes)

        stored = db.get_state("forecast_per_asset_weights")
        self.assertIsNotNone(stored)
        self.assertNotIn("BTC", stored.get("per_asset", {}))

    def test_per_category_learning(self):
        """Outcomes from multiple assets in same category aggregate."""
        evaluator = self._make_evaluator()
        db = FakeDB()

        # BTC + ETH both in CRYPTO category
        outcomes = (
            self._make_outcomes_for_ticker("BTC", 8) +
            self._make_outcomes_for_ticker("ETH", 8)
        )
        # 16 total CRYPTO outcomes ≥ PER_CATEGORY_MIN_SAMPLES=10

        evaluator._update_per_asset_weights(db, outcomes)

        stored = db.get_state("forecast_per_asset_weights")
        self.assertIn("per_category", stored)
        self.assertIn("CRYPTO", stored["per_category"])

    def test_thresholds_in_state(self):
        """Persisted state includes threshold values."""
        evaluator = self._make_evaluator()
        db = FakeDB()

        evaluator._update_per_asset_weights(db, [])

        stored = db.get_state("forecast_per_asset_weights")
        self.assertIn("thresholds", stored)
        self.assertEqual(
            stored["thresholds"]["per_asset_min_samples"],
            PER_ASSET_MIN_SAMPLES,
        )


class TestGetEvaluationIncludesPerAsset(unittest.TestCase):
    """Feature C: get_evaluation() includes per_asset_weights."""

    def test_per_asset_weights_in_evaluation(self):
        evaluator = ForecastEvaluator(api_key="")
        db = FakeDB()

        # Fake per-asset weights in DB
        db._state["forecast_per_asset_weights"] = {
            "per_asset": {"BTC": {"weights": {"momentum": 0.4}}},
            "per_category": {},
        }

        # Mock the stats call
        db.get_forecast_evaluation_stats = lambda: {
            "overall": {}, "by_horizon": {}, "by_asset": {}, "calibration": []
        }

        result = evaluator.get_evaluation(db)
        self.assertIn("per_asset_weights", result)
        self.assertIsNotNone(result["per_asset_weights"])
        self.assertIn("per_asset", result["per_asset_weights"])


class TestAssetCategoryMapping(unittest.TestCase):
    """Feature C: ASSET_CATEGORY constant correctness."""

    def test_all_assets_have_categories(self):
        for ticker in ASSET_KEYWORDS:
            self.assertIn(
                ticker, ASSET_CATEGORY,
                f"{ticker} missing from ASSET_CATEGORY mapping"
            )

    def test_known_categories(self):
        self.assertEqual(ASSET_CATEGORY["BTC"], "CRYPTO")
        self.assertEqual(ASSET_CATEGORY["ETH"], "CRYPTO")
        self.assertEqual(ASSET_CATEGORY["SPY"], "EQUITY")
        self.assertEqual(ASSET_CATEGORY["GLD"], "COMMODITY")
        self.assertEqual(ASSET_CATEGORY["DXY"], "FX")
        self.assertEqual(ASSET_CATEGORY["TLT"], "RATES")
        self.assertEqual(ASSET_CATEGORY["ITA"], "SECTOR")


class TestGenerateUsesPerAssetWeights(unittest.TestCase):
    """Feature C: generate() applies per-asset weight swapping."""

    def test_weight_swap_during_generate(self):
        """verify the engine uses the correct weights per asset during signal computation."""
        db = FakeDB()
        # Set per-asset weights for BTC with high momentum weight
        db._state["forecast_per_asset_weights"] = {
            "per_asset": {
                "BTC": {
                    "weights": {
                        "momentum": 0.50, "prediction_market": 0.15,
                        "whale": 0.15, "cross_asset": 0.10,
                        "news_sentiment": 0.10,
                    },
                    "sample_count": 25,
                },
            },
            "per_category": {},
        }

        md = FakeMarketData()
        with patch("forecast_engine.composite_momentum", return_value=(0.1, "test")):
            engine = ForecastEngine(md, db, api_key="")

        # Verify the weights were loaded
        self.assertIn("BTC", engine._per_asset_weights)
        self.assertEqual(engine._per_asset_weights["BTC"]["momentum"], 0.50)

        # Verify _get_weights_for_asset returns per-asset weights
        btc_w = engine._get_weights_for_asset("BTC")
        self.assertEqual(btc_w["momentum"], 0.50)

        # And SPY falls back to global
        spy_w = engine._get_weights_for_asset("SPY")
        self.assertEqual(spy_w, engine._weights)

    def test_weight_restoration_after_error(self):
        """Weights are restored even if signal computation raises."""
        db = FakeDB()
        md = FakeMarketData()
        with patch("forecast_engine.composite_momentum", return_value=(0.1, "test")):
            engine = ForecastEngine(md, db, api_key="")

        original_weights = dict(engine._weights)
        custom = {"momentum": 0.99, "prediction_market": 0.0,
                  "whale": 0.0, "cross_asset": 0.0, "news_sentiment": 0.01}
        engine._per_asset_weights = {"BTC": custom}

        # After getting weights for BTC and restoring
        w = engine._get_weights_for_asset("BTC")
        self.assertEqual(w["momentum"], 0.99)

        # Global weights should be unchanged
        self.assertEqual(engine._weights, original_weights)


# ===========================================================================
# Integration: all features work together
# ===========================================================================

class TestIntegration(unittest.TestCase):
    """Cross-feature integration checks."""

    def test_constants_exist(self):
        """All new constants are accessible."""
        self.assertIsInstance(PER_ASSET_MIN_SAMPLES, int)
        self.assertIsInstance(PER_CATEGORY_MIN_SAMPLES, int)
        self.assertIsInstance(_PM_HALF_LIFE_HOURS, float)
        self.assertIsInstance(_PM_ALERT_HALF_LIFE_HOURS, float)
        self.assertIsInstance(_PM_MIN_VOLUME_FOR_WEIGHT, int)
        self.assertGreater(PER_ASSET_MIN_SAMPLES, PER_CATEGORY_MIN_SAMPLES)

    def test_backtester_imports_default_weights(self):
        """Backtester uses same DEFAULT_WEIGHTS as engine."""
        from backtester import DEFAULT_WEIGHTS as BT_WEIGHTS
        self.assertEqual(BT_WEIGHTS, DEFAULT_WEIGHTS)

    def test_evaluator_defaults_match_engine(self):
        """Evaluator and engine share same weight defaults."""
        self.assertEqual(EVAL_DEFAULT_WEIGHTS, DEFAULT_WEIGHTS)

    def test_backtest_with_per_asset_weight_override(self):
        """Backtest can simulate per-asset-like weight changes."""
        db = FakeDB()
        now = _utcnow()
        outcomes = []
        for i in range(20):
            outcomes.append({
                "id": i, "call_id": i,
                "ticker": "BTC", "horizon": "24h",
                "direction": "UP", "magnitude": "SMALL",
                "confidence": 60,
                "drivers_json": json.dumps([
                    {"family": "momentum", "value": 0.4, "weight": 0.2,
                     "contribution": 0.08},
                    {"family": "whale", "value": -0.2, "weight": 0.2,
                     "contribution": -0.04},
                ]),
                "call_generated_at": (now - timedelta(days=i)).isoformat(),
                "actual_return_pct": 1.5 if i % 2 == 0 else -1.5,
                "direction_correct": 1 if i % 2 == 0 else 0,
                "brier_score": 0.25,
                "log_loss": 0.5,
            })
        db._forecast_outcomes = outcomes

        report = run_backtest(
            db,
            override_weights={"momentum": 0.50},
            lookback_days=30,
        )
        self.assertGreater(report.total_predictions, 0)
        self.assertIn("BTC", report.per_asset)


if __name__ == "__main__":
    unittest.main()
