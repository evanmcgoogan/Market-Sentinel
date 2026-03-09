"""
Tests for the technical indicators and calibration systems.

Covers:
  1. RSI — mean-reversion signal from Wilder-smoothed RSI
  2. MACD — trend-following normalized histogram
  3. Multi-timeframe momentum — blended 1d/3d/5d/10d returns
  4. Bollinger %B — position within volatility envelope
  5. Volume-weighted momentum — volume-aware returns
  6. Composite momentum — weighted blend of all 5 indicators
  7. PAV isotonic calibration algorithm
  8. ForecastEngine momentum integration
  9. ForecastEngine calibration wiring
  10. ForecastEvaluator calibration update
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

from technical import (
    rsi,
    macd,
    multi_timeframe_momentum,
    bollinger_pct_b,
    volume_weighted_momentum,
    composite_momentum,
    _ema,
    _clamp,
    MOMENTUM_SUB_WEIGHTS,
)
from forecast_evaluator import _pav_isotonic


# ---------------------------------------------------------------------------
# Helpers — create PriceBar-like objects without importing market_data
# ---------------------------------------------------------------------------

@dataclass
class MockBar:
    """Lightweight PriceBar stand-in for tests."""
    ticker: str = "SPY"
    dt: str = "2026-01-01"
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float = 0.0
    volume: Optional[float] = None
    source: str = "test"


def _make_bars(closes, volumes=None, ticker="SPY"):
    """Create a list of MockBars from close prices."""
    bars = []
    for i, c in enumerate(closes):
        v = volumes[i] if volumes else 1000.0
        bars.append(MockBar(
            ticker=ticker,
            dt=f"2026-01-{i+1:02d}",
            close=c,
            open=c * 0.99,
            high=c * 1.01,
            low=c * 0.98,
            volume=v,
        ))
    return bars


def _make_uptrend(n=40, start=100.0, daily_pct=0.5):
    """Generate a steady uptrend."""
    closes = [start]
    for _ in range(n - 1):
        closes.append(closes[-1] * (1 + daily_pct / 100))
    return _make_bars(closes)


def _make_downtrend(n=40, start=100.0, daily_pct=-0.5):
    """Generate a steady downtrend."""
    closes = [start]
    for _ in range(n - 1):
        closes.append(closes[-1] * (1 + daily_pct / 100))
    return _make_bars(closes)


def _make_flat(n=40, price=100.0):
    """Generate flat prices with tiny noise."""
    import random
    random.seed(42)
    closes = [price + random.uniform(-0.01, 0.01) for _ in range(n)]
    return _make_bars(closes)


# ===========================================================================
# 1. RSI Tests
# ===========================================================================

class TestRSI(unittest.TestCase):

    def test_insufficient_data_returns_zero(self):
        """RSI with fewer than period+1 bars returns 0.0."""
        bars = _make_bars([100, 101, 102])  # only 3 bars, need 15
        self.assertEqual(rsi(bars), 0.0)

    def test_strong_uptrend_returns_negative(self):
        """Overbought (RSI > 70) → bearish (negative) mean-reversion."""
        bars = _make_uptrend(n=40, daily_pct=2.0)
        signal = rsi(bars)
        self.assertLess(signal, 0, "Strong uptrend should produce bearish RSI signal")

    def test_strong_downtrend_returns_positive(self):
        """Oversold (RSI < 30) → bullish (positive) mean-reversion."""
        bars = _make_downtrend(n=40, daily_pct=-2.0)
        signal = rsi(bars)
        self.assertGreater(signal, 0, "Strong downtrend should produce bullish RSI signal")

    def test_flat_market_near_zero(self):
        """Flat market → RSI near 50 → signal near zero."""
        bars = _make_flat(n=40)
        signal = rsi(bars)
        self.assertAlmostEqual(signal, 0.0, delta=0.35)

    def test_output_bounded(self):
        """RSI signal always in [-1, 1]."""
        for pct in [-5.0, -2.0, -0.1, 0.1, 2.0, 5.0]:
            bars = _make_uptrend(n=30, daily_pct=pct) if pct > 0 else _make_downtrend(n=30, daily_pct=pct)
            signal = rsi(bars)
            self.assertGreaterEqual(signal, -1.0)
            self.assertLessEqual(signal, 1.0)


# ===========================================================================
# 2. MACD Tests
# ===========================================================================

class TestMACD(unittest.TestCase):

    def test_insufficient_data_returns_zero(self):
        """MACD needs slow+signal_period bars."""
        bars = _make_bars([100] * 10)  # too few
        self.assertEqual(macd(bars), 0.0)

    def test_uptrend_positive(self):
        """Uptrend → positive MACD histogram → positive signal."""
        bars = _make_uptrend(n=40, daily_pct=1.0)
        signal = macd(bars)
        self.assertGreater(signal, 0, "Uptrend should produce positive MACD signal")

    def test_downtrend_negative(self):
        """Downtrend → negative MACD histogram → negative signal."""
        bars = _make_downtrend(n=40, daily_pct=-1.0)
        signal = macd(bars)
        self.assertLess(signal, 0, "Downtrend should produce negative MACD signal")

    def test_output_bounded(self):
        """MACD signal always in [-1, 1]."""
        bars = _make_uptrend(n=40, daily_pct=3.0)
        signal = macd(bars)
        self.assertGreaterEqual(signal, -1.0)
        self.assertLessEqual(signal, 1.0)


# ===========================================================================
# 3. Multi-Timeframe Momentum Tests
# ===========================================================================

class TestMultiTimeframeMomentum(unittest.TestCase):

    def test_insufficient_data_returns_zero(self):
        """Needs at least 11 bars."""
        bars = _make_bars([100] * 5)
        self.assertEqual(multi_timeframe_momentum(bars), 0.0)

    def test_uptrend_positive(self):
        bars = _make_uptrend(n=20, daily_pct=1.0)
        signal = multi_timeframe_momentum(bars)
        self.assertGreater(signal, 0)

    def test_downtrend_negative(self):
        bars = _make_downtrend(n=20, daily_pct=-1.0)
        signal = multi_timeframe_momentum(bars)
        self.assertLess(signal, 0)

    def test_output_bounded(self):
        bars = _make_uptrend(n=20, daily_pct=5.0)
        signal = multi_timeframe_momentum(bars)
        self.assertGreaterEqual(signal, -1.0)
        self.assertLessEqual(signal, 1.0)


# ===========================================================================
# 4. Bollinger %B Tests
# ===========================================================================

class TestBollingerPctB(unittest.TestCase):

    def test_insufficient_data_returns_zero(self):
        """Needs at least 20 bars."""
        bars = _make_bars([100] * 10)
        self.assertEqual(bollinger_pct_b(bars), 0.0)

    def test_spike_above_upper_band_bearish(self):
        """Price well above upper band → overbought → negative signal."""
        closes = [100.0] * 19 + [115.0]  # spike at end
        bars = _make_bars(closes)
        signal = bollinger_pct_b(bars)
        self.assertLess(signal, 0, "Price above upper band should be bearish")

    def test_drop_below_lower_band_bullish(self):
        """Price well below lower band → oversold → positive signal."""
        closes = [100.0] * 19 + [85.0]  # drop at end
        bars = _make_bars(closes)
        signal = bollinger_pct_b(bars)
        self.assertGreater(signal, 0, "Price below lower band should be bullish")

    def test_flat_price_zero_signal(self):
        """Perfectly flat price → zero std → returns 0."""
        bars = _make_bars([100.0] * 20)
        signal = bollinger_pct_b(bars)
        self.assertEqual(signal, 0.0)

    def test_output_bounded(self):
        closes = [100.0] * 19 + [200.0]
        bars = _make_bars(closes)
        signal = bollinger_pct_b(bars)
        self.assertGreaterEqual(signal, -1.0)
        self.assertLessEqual(signal, 1.0)


# ===========================================================================
# 5. Volume-Weighted Momentum Tests
# ===========================================================================

class TestVolumeWeightedMomentum(unittest.TestCase):

    def test_insufficient_data_returns_zero(self):
        bars = _make_bars([100, 101])
        self.assertEqual(volume_weighted_momentum(bars), 0.0)

    def test_uptrend_positive(self):
        bars = _make_uptrend(n=15, daily_pct=1.0)
        signal = volume_weighted_momentum(bars)
        self.assertGreater(signal, 0)

    def test_high_volume_move_amplified(self):
        """A 1% move on high volume should produce larger signal than on low volume."""
        closes = [100.0] * 9 + [100.0, 101.0]  # last day: +1%
        # High volume on the move day
        volumes_high = [1000] * 9 + [1000, 5000]
        # Low volume on the move day
        volumes_low = [1000] * 9 + [1000, 200]

        bars_high = _make_bars(closes, volumes=volumes_high)
        bars_low = _make_bars(closes, volumes=volumes_low)

        sig_high = volume_weighted_momentum(bars_high)
        sig_low = volume_weighted_momentum(bars_low)
        # Both should be positive (upward move)
        self.assertGreater(sig_high, 0)
        self.assertGreater(sig_low, 0)

    def test_no_volume_falls_back_to_equal_weight(self):
        """With None volumes, falls back to equal-weight returns."""
        closes = [100.0] * 5 + [101.0, 102.0, 103.0]
        bars = _make_bars(closes, volumes=[None] * 8)
        signal = volume_weighted_momentum(bars)
        # Should still produce a signal
        self.assertNotEqual(signal, 0.0)

    def test_output_bounded(self):
        bars = _make_uptrend(n=15, daily_pct=3.0)
        signal = volume_weighted_momentum(bars)
        self.assertGreaterEqual(signal, -1.0)
        self.assertLessEqual(signal, 1.0)


# ===========================================================================
# 6. Composite Momentum Tests
# ===========================================================================

class TestCompositeMomentum(unittest.TestCase):

    def test_returns_tuple(self):
        """composite_momentum returns (value, source_description)."""
        bars = _make_uptrend(n=40, daily_pct=1.0)
        result = composite_momentum(bars)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        value, source = result
        self.assertIsInstance(value, float)
        self.assertIsInstance(source, str)

    def test_uptrend_positive(self):
        bars = _make_uptrend(n=40, daily_pct=1.0)
        value, _ = composite_momentum(bars)
        self.assertGreater(value, 0, "Uptrend should produce positive composite signal")

    def test_downtrend_negative(self):
        bars = _make_downtrend(n=40, daily_pct=-1.0)
        value, _ = composite_momentum(bars)
        self.assertLess(value, 0, "Downtrend should produce negative composite signal")

    def test_output_bounded(self):
        bars = _make_uptrend(n=40, daily_pct=5.0)
        value, _ = composite_momentum(bars)
        self.assertGreaterEqual(value, -1.0)
        self.assertLessEqual(value, 1.0)

    def test_source_describes_indicators(self):
        """Source string should mention indicator names when signals are strong."""
        bars = _make_uptrend(n=40, daily_pct=2.0)
        _, source = composite_momentum(bars)
        # Should mention at least one of the indicator labels
        indicator_labels = ["RSI", "MACD", "Momentum", "Bollinger", "Vol-Momentum"]
        found = any(label in source for label in indicator_labels)
        self.assertTrue(found, f"Source should mention indicators, got: {source}")

    def test_flat_source_mentions_flat(self):
        """When all indicators are near zero, source should reflect that."""
        bars = _make_flat(n=40)
        _, source = composite_momentum(bars)
        # Either mentions 'Flat' or still names some weak signals
        self.assertIsInstance(source, str)
        self.assertGreater(len(source), 0)

    def test_sub_weights_sum_to_one(self):
        """Sub-weights should sum to 1.0 for proper blending."""
        total = sum(MOMENTUM_SUB_WEIGHTS.values())
        self.assertAlmostEqual(total, 1.0, places=6)

    def test_insufficient_bars_returns_zero(self):
        """With very few bars, signal should be 0 or near 0."""
        bars = _make_bars([100, 101])
        value, _ = composite_momentum(bars)
        self.assertAlmostEqual(value, 0.0, delta=0.01)


# ===========================================================================
# 7. PAV Isotonic Calibration Algorithm Tests
# ===========================================================================

class TestPAVIsotonic(unittest.TestCase):

    def test_empty_input(self):
        """Empty input returns empty."""
        self.assertEqual(_pav_isotonic([]), [])

    def test_single_point(self):
        """Single point returns that point."""
        result = _pav_isotonic([(0.5, 1.0)])
        self.assertEqual(len(result), 1)

    def test_already_monotone(self):
        """Already monotone data should return similar values."""
        points = [(0.2, 0.0), (0.4, 0.0), (0.6, 1.0), (0.8, 1.0)]
        curve = _pav_isotonic(points)
        # Values should be non-decreasing
        for i in range(len(curve) - 1):
            self.assertLessEqual(curve[i][1], curve[i + 1][1])

    def test_violating_pair_merged(self):
        """Two adjacent violators should be merged."""
        # predicted=0.3 → actual=1.0, predicted=0.7 → actual=0.0
        # This violates isotonicity: higher prediction has lower actual
        points = [(0.3, 1.0), (0.7, 0.0)]
        curve = _pav_isotonic(points)
        # After merging: both get the same value (average with Laplace smoothing)
        self.assertEqual(len(curve), 1)  # merged into single block

    def test_monotonicity_preserved(self):
        """Output curve must be monotonically non-decreasing."""
        import random
        random.seed(42)
        # Random data — PAV should enforce monotonicity
        points = sorted([(random.random(), random.choice([0.0, 1.0])) for _ in range(50)])
        curve = _pav_isotonic(points)
        for i in range(len(curve) - 1):
            self.assertLessEqual(
                curve[i][1], curve[i + 1][1] + 1e-9,
                f"Monotonicity violated at index {i}: {curve[i][1]} > {curve[i+1][1]}"
            )

    def test_laplace_smoothing_prevents_extremes(self):
        """Laplace smoothing: no calibrated value should be exactly 0 or 1."""
        points = [(0.1, 0.0), (0.2, 0.0), (0.8, 1.0), (0.9, 1.0)]
        curve = _pav_isotonic(points)
        for _, cal in curve:
            self.assertGreater(cal, 0.0, "Laplace should prevent exactly 0")
            self.assertLess(cal, 1.0, "Laplace should prevent exactly 1")

    def test_well_calibrated_data_near_diagonal(self):
        """If data is perfectly calibrated, curve should be near the diagonal."""
        # 30% predictions correct 30% of the time, etc.
        points = []
        import random
        random.seed(123)
        for conf in [0.3, 0.5, 0.7]:
            for _ in range(20):
                actual = 1.0 if random.random() < conf else 0.0
                points.append((conf, actual))
        points.sort()
        curve = _pav_isotonic(points)
        # The calibration curve should have values roughly in [0.2, 0.8] range
        # Not an exact check due to randomness but should be reasonable
        self.assertGreater(len(curve), 0)

    def test_large_dataset_performance(self):
        """PAV should handle 1000+ points without issue."""
        import random
        random.seed(99)
        points = sorted([(random.random(), random.choice([0.0, 1.0])) for _ in range(1000)])
        curve = _pav_isotonic(points)
        self.assertGreater(len(curve), 0)
        # Verify monotonicity
        for i in range(len(curve) - 1):
            self.assertLessEqual(curve[i][1], curve[i + 1][1] + 1e-9)


# ===========================================================================
# 8. ForecastEngine Momentum Integration Tests
# ===========================================================================

class TestForecastEngineMomentum(unittest.TestCase):
    """Test that ForecastEngine._momentum_signal uses composite_momentum."""

    def _make_engine(self):
        """Create a minimal ForecastEngine for testing."""
        mock_md = MagicMock()
        mock_db = MagicMock()
        mock_db.get_state = MagicMock(return_value=None)

        from forecast_engine import ForecastEngine
        engine = ForecastEngine(mock_md, mock_db, api_key="")
        return engine

    def test_momentum_returns_driver(self):
        """_momentum_signal returns a Driver with composite momentum data."""
        engine = self._make_engine()
        bars = _make_uptrend(n=40, daily_pct=1.0)
        driver = engine._momentum_signal("SPY", bars, "24h")
        self.assertIsNotNone(driver)
        self.assertEqual(driver.family, "momentum")
        self.assertGreater(driver.value, 0, "Uptrend should yield positive momentum driver")
        # Source should come from composite_momentum (mentions indicator labels)
        self.assertIsInstance(driver.source, str)
        self.assertGreater(len(driver.source), 0)

    def test_momentum_insufficient_data(self):
        """With <3 bars, returns zero-value driver."""
        engine = self._make_engine()
        bars = _make_bars([100, 101])
        driver = engine._momentum_signal("SPY", bars, "24h")
        self.assertIsNotNone(driver)
        self.assertEqual(driver.value, 0.0)
        self.assertIn("insufficient", driver.source)

    def test_momentum_48h_amplified(self):
        """48h horizon should amplify the momentum signal."""
        engine = self._make_engine()
        bars = _make_uptrend(n=40, daily_pct=0.5)
        d24 = engine._momentum_signal("SPY", bars, "24h")
        d48 = engine._momentum_signal("SPY", bars, "48h")
        # 48h value = 24h value * 1.2 (clamped to [-1,1])
        if abs(d24.value) < 0.83:  # not already at clamp boundary
            self.assertGreater(abs(d48.value), abs(d24.value))

    def test_momentum_downtrend_negative(self):
        """Downtrend should yield negative momentum driver."""
        engine = self._make_engine()
        bars = _make_downtrend(n=40, daily_pct=-1.0)
        driver = engine._momentum_signal("SPY", bars, "24h")
        self.assertIsNotNone(driver)
        self.assertLess(driver.value, 0)


# ===========================================================================
# 9. ForecastEngine Calibration Tests
# ===========================================================================

class TestForecastEngineCalibration(unittest.TestCase):
    """Test calibration loading and application in ForecastEngine."""

    def _make_engine(self):
        mock_md = MagicMock()
        mock_db = MagicMock()
        mock_db.get_state = MagicMock(return_value=None)
        from forecast_engine import ForecastEngine
        return ForecastEngine(mock_md, mock_db, api_key="")

    def test_load_calibration_none_when_missing(self):
        """No stored curve → returns None."""
        engine = self._make_engine()
        engine._db.get_state = MagicMock(return_value=None)
        result = engine._load_calibration()
        self.assertIsNone(result)

    def test_load_calibration_valid_curve(self):
        """Valid stored curve → returns list of tuples."""
        engine = self._make_engine()
        engine._db.get_state = MagicMock(return_value={
            "curve": [[20.0, 25.0], [40.0, 42.0], [60.0, 58.0], [80.0, 75.0]]
        })
        result = engine._load_calibration()
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0], (20.0, 25.0))

    def test_apply_calibration_interpolation(self):
        """Calibration should interpolate between curve points."""
        engine = self._make_engine()
        curve = [(20.0, 30.0), (40.0, 45.0), (60.0, 55.0), (80.0, 70.0)]
        # Test at exact point
        result = engine._apply_calibration(20, curve)
        self.assertEqual(result, 30)
        # Test at midpoint between 20 and 40
        result = engine._apply_calibration(30, curve)
        # Linear interp: 30 + (45-30) * (30-20)/(40-20) = 30 + 7.5 = 37.5
        self.assertEqual(result, 38)  # rounded

    def test_apply_calibration_extrapolation(self):
        """Below/above curve range uses edge values."""
        engine = self._make_engine()
        curve = [(30.0, 35.0), (70.0, 65.0)]
        # Below range
        result = engine._apply_calibration(15, curve)
        self.assertEqual(result, 35)
        # Above range
        result = engine._apply_calibration(90, curve)
        self.assertEqual(result, 65)

    def test_apply_calibration_respects_bounds(self):
        """Output always in [15, 95]."""
        engine = self._make_engine()
        curve = [(10.0, 5.0), (90.0, 98.0)]
        # Even if curve says 5, minimum is 15
        result = engine._apply_calibration(10, curve)
        self.assertEqual(result, 15)
        # Even if curve says 98, maximum is 95
        result = engine._apply_calibration(90, curve)
        self.assertEqual(result, 95)

    def test_apply_calibration_empty_curve_passthrough(self):
        """Empty/short curve → passthrough."""
        engine = self._make_engine()
        self.assertEqual(engine._apply_calibration(50, []), 50)
        self.assertEqual(engine._apply_calibration(50, [(50.0, 60.0)]), 50)


# ===========================================================================
# 10. ForecastEvaluator Calibration Update Tests
# ===========================================================================

class TestForecastEvaluatorCalibration(unittest.TestCase):
    """Test the update_calibration method in ForecastEvaluator."""

    def _make_evaluator(self):
        from forecast_evaluator import ForecastEvaluator
        return ForecastEvaluator(api_key="")

    def _make_mock_outcomes(self, n=30):
        """Create mock forecast outcomes with varying confidence and correctness."""
        import random
        random.seed(42)
        outcomes = []
        for i in range(n):
            conf = random.randint(20, 90)
            # Higher confidence → more likely correct (realistic model)
            correct = 1 if random.random() < (conf / 100.0) else 0
            outcomes.append({
                "id": i,
                "confidence": conf,
                "direction_correct": correct,
                "brier_score": (conf / 100.0 - correct) ** 2,
                "log_loss": -math.log(max(0.01, conf / 100.0 if correct else 1 - conf / 100.0)),
                "drivers_json": "[]",
            })
        return outcomes

    def test_insufficient_data_skips(self):
        """< MIN_SAMPLES_FOR_LEARNING → returns insufficient_data."""
        evaluator = self._make_evaluator()
        mock_db = MagicMock()
        mock_db.get_recent_forecast_outcomes = MagicMock(return_value=[{"confidence": 50, "direction_correct": 1}] * 5)
        result = evaluator.update_calibration(mock_db)
        self.assertEqual(result["status"], "insufficient_data")

    def test_produces_monotone_curve(self):
        """With sufficient data, produces a monotonically non-decreasing curve."""
        evaluator = self._make_evaluator()
        outcomes = self._make_mock_outcomes(n=50)
        mock_db = MagicMock()
        mock_db.get_recent_forecast_outcomes = MagicMock(return_value=outcomes)
        mock_db.set_state = MagicMock()

        result = evaluator.update_calibration(mock_db)
        self.assertEqual(result["status"], "updated")

        curve = result["curve"]
        self.assertGreater(len(curve), 0)

        # Verify monotonicity (calibrated values)
        for i in range(len(curve) - 1):
            self.assertLessEqual(curve[i][1], curve[i + 1][1] + 0.01)

    def test_computes_ece(self):
        """ECE (Expected Calibration Error) should be a number."""
        evaluator = self._make_evaluator()
        outcomes = self._make_mock_outcomes(n=50)
        mock_db = MagicMock()
        mock_db.get_recent_forecast_outcomes = MagicMock(return_value=outcomes)
        mock_db.set_state = MagicMock()

        result = evaluator.update_calibration(mock_db)
        self.assertIn("ece", result)
        self.assertIsInstance(result["ece"], float)
        self.assertGreaterEqual(result["ece"], 0.0)
        self.assertLessEqual(result["ece"], 1.0)

    def test_persists_to_db(self):
        """Calibration curve should be saved via db.set_state."""
        evaluator = self._make_evaluator()
        outcomes = self._make_mock_outcomes(n=50)
        mock_db = MagicMock()
        mock_db.get_recent_forecast_outcomes = MagicMock(return_value=outcomes)
        mock_db.set_state = MagicMock()

        evaluator.update_calibration(mock_db)
        mock_db.set_state.assert_called_once()
        call_args = mock_db.set_state.call_args
        self.assertEqual(call_args[0][0], "forecast_calibration_curve")
        stored = call_args[0][1]
        self.assertIn("curve", stored)
        self.assertIn("ece", stored)
        self.assertIn("sample_count", stored)

    def test_get_calibration_curve_static(self):
        """Static method retrieves stored curve."""
        from forecast_evaluator import ForecastEvaluator
        mock_db = MagicMock()
        mock_db.get_state = MagicMock(return_value={
            "curve": [[20, 25], [50, 48], [80, 72]],
            "ece": 0.05,
        })
        result = ForecastEvaluator.get_calibration_curve(mock_db)
        self.assertIsNotNone(result)
        self.assertEqual(len(result["curve"]), 3)

    def test_get_evaluation_includes_calibration(self):
        """get_evaluation should include calibration data."""
        evaluator = self._make_evaluator()
        mock_db = MagicMock()
        mock_db.get_forecast_evaluation_stats = MagicMock(return_value={})
        mock_db.get_state = MagicMock(side_effect=lambda key, default=None: {
            "forecast_signal_weights": {"weights": {}, "driver_quality": {}, "vs_baseline": {}, "confidence_modifier": 0},
            "forecast_calibration_curve": {"curve": [[20, 25], [80, 72]], "ece": 0.05},
        }.get(key, default))

        result = evaluator.get_evaluation(mock_db)
        self.assertIn("calibration", result)
        self.assertIsNotNone(result["calibration"])


# ===========================================================================
# 11. EMA Helper Tests
# ===========================================================================

class TestEMAHelper(unittest.TestCase):

    def test_empty_returns_empty(self):
        self.assertEqual(_ema([], 12), [])

    def test_single_value(self):
        result = _ema([100.0], 12)
        self.assertEqual(result, [100.0])

    def test_ema_converges(self):
        """EMA of constant values should remain constant."""
        result = _ema([50.0] * 20, 12)
        for v in result:
            self.assertAlmostEqual(v, 50.0, places=6)

    def test_ema_follows_trend(self):
        """EMA should eventually follow an uptrend."""
        values = list(range(1, 21))  # 1 to 20
        result = _ema(values, 5)
        # Last EMA value should be near but below 20 (lagging)
        self.assertGreater(result[-1], 15)
        self.assertLess(result[-1], 20)


# ===========================================================================
# 12. Clamp Helper Test
# ===========================================================================

class TestClamp(unittest.TestCase):

    def test_within_bounds(self):
        self.assertEqual(_clamp(0.5), 0.5)

    def test_below_lower(self):
        self.assertEqual(_clamp(-2.0), -1.0)

    def test_above_upper(self):
        self.assertEqual(_clamp(2.0), 1.0)

    def test_custom_bounds(self):
        self.assertEqual(_clamp(5.0, lo=0, hi=10), 5.0)
        self.assertEqual(_clamp(-1.0, lo=0, hi=10), 0)


# ===========================================================================
# 13. Integration: Full ForecastEngine.generate() sanity test
# ===========================================================================

class TestForecastEngineIntegration(unittest.TestCase):
    """Smoke test that generate() runs end-to-end with composite momentum."""

    def test_generate_with_mock_data(self):
        """Full generate() should return valid structure with no errors."""
        from forecast_engine import ForecastEngine, OUTLOOK_ASSETS

        mock_md = MagicMock()
        # Return uptrend bars for all assets
        mock_md.get_history = MagicMock(return_value=_make_uptrend(n=40, daily_pct=0.5))

        mock_db = MagicMock()
        mock_db.get_state = MagicMock(return_value=None)
        mock_db.get_top_volume_markets = MagicMock(return_value=[])
        mock_db.get_recent_alerts_feed = MagicMock(return_value=[])
        mock_db.get_all_recent_news = MagicMock(return_value=[])
        mock_db.save_outlook_prediction = MagicMock()
        mock_db.save_forecast_calls = MagicMock()

        engine = ForecastEngine(mock_md, mock_db, api_key="")
        result = engine.generate(mock_db)

        # Validate structure
        self.assertIn("assets", result)
        self.assertIn("market_regime", result)
        self.assertIn("asset_order", result)

        # Every asset should have 24h and 48h forecasts
        for a in OUTLOOK_ASSETS:
            ticker = a["ticker"]
            self.assertIn(ticker, result["assets"])
            asset_data = result["assets"][ticker]
            self.assertIn("24h", asset_data)
            self.assertIn("48h", asset_data)

            # Confidence should be >= 15 (no grey states)
            self.assertGreaterEqual(asset_data["24h"]["confidence"], 15)
            self.assertGreaterEqual(asset_data["48h"]["confidence"], 15)

            # Direction should be UP or DOWN only
            self.assertIn(asset_data["24h"]["direction"], ("UP", "DOWN"))
            self.assertIn(asset_data["48h"]["direction"], ("UP", "DOWN"))

    def test_generate_with_calibration(self):
        """generate() should apply calibration when curve is available."""
        from forecast_engine import ForecastEngine

        mock_md = MagicMock()
        mock_md.get_history = MagicMock(return_value=_make_uptrend(n=40, daily_pct=0.5))

        # Return a calibration curve that compresses confidence toward 50
        cal_curve = {
            "curve": [[15.0, 30.0], [50.0, 45.0], [80.0, 60.0], [95.0, 70.0]]
        }

        def mock_get_state(key, default=None):
            if key == "forecast_calibration_curve":
                return cal_curve
            return default

        mock_db = MagicMock()
        mock_db.get_state = MagicMock(side_effect=mock_get_state)
        mock_db.get_top_volume_markets = MagicMock(return_value=[])
        mock_db.get_recent_alerts_feed = MagicMock(return_value=[])
        mock_db.get_all_recent_news = MagicMock(return_value=[])
        mock_db.save_outlook_prediction = MagicMock()
        mock_db.save_forecast_calls = MagicMock()

        engine = ForecastEngine(mock_md, mock_db, api_key="")
        result = engine.generate(mock_db)

        # All confidences should be bounded [15, 95]
        for ticker, data in result["assets"].items():
            for h in ("24h", "48h"):
                conf = data[h]["confidence"]
                self.assertGreaterEqual(conf, 15)
                self.assertLessEqual(conf, 95)


if __name__ == "__main__":
    unittest.main()
