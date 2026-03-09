"""
Technical Indicators — pure functions for OHLCV-based analysis.

Every function takes a list of PriceBar objects and returns a signal
in [-1, 1].  No side effects, no external dependencies, no state.

Used by ForecastEngine to build a rich composite momentum signal.
"""

import math
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Sub-weights for blending indicators into composite momentum
# ---------------------------------------------------------------------------

MOMENTUM_SUB_WEIGHTS = {
    "rsi":          0.15,
    "macd":         0.20,
    "multi_tf":     0.25,
    "bollinger":    0.15,
    "vol_weighted": 0.25,
}


# ---------------------------------------------------------------------------
# Helper: extract close prices from bar-like objects
# ---------------------------------------------------------------------------

def _closes(bars) -> List[float]:
    """Extract valid close prices from PriceBar objects."""
    return [b.close for b in bars if b.close is not None and b.close > 0]


def _volumes(bars) -> List[Optional[float]]:
    """Extract volume values (may be None) from PriceBar objects."""
    return [getattr(b, "volume", None) for b in bars]


def _clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


# ---------------------------------------------------------------------------
# 1. RSI — Relative Strength Index (Wilder smoothing)
# ---------------------------------------------------------------------------

def rsi(bars, period: int = 14) -> float:
    """
    Compute RSI and map to a mean-reversion signal in [-1, 1].

    RSI < 30  →  bullish (positive): oversold, expect bounce
    RSI > 70  →  bearish (negative): overbought, expect pullback
    RSI ≈ 50  →  near zero

    Requires at least period+1 bars. Returns 0.0 if insufficient data.
    """
    closes = _closes(bars)
    if len(closes) < period + 1:
        return 0.0

    # Compute price changes
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]

    # Initial average gain/loss (simple average of first `period` changes)
    gains = [max(c, 0) for c in changes[:period]]
    losses = [max(-c, 0) for c in changes[:period]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    # Wilder smoothing for remaining changes
    for c in changes[period:]:
        avg_gain = (avg_gain * (period - 1) + max(c, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-c, 0)) / period

    # RSI calculation
    if avg_loss < 1e-10:
        rsi_value = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_value = 100.0 - 100.0 / (1.0 + rs)

    # Map to [-1, 1] with mean-reversion logic
    if rsi_value < 30:
        # Oversold → bullish signal (positive)
        signal = (30.0 - rsi_value) / 30.0  # 0 to 1.0
    elif rsi_value > 70:
        # Overbought → bearish signal (negative)
        signal = -(rsi_value - 70.0) / 30.0  # -1.0 to 0
    else:
        # Neutral zone: mild mean-reversion toward 50
        signal = (50.0 - rsi_value) / 50.0 * 0.3  # -0.3 to +0.3

    return _clamp(signal)


# ---------------------------------------------------------------------------
# 2. MACD — Moving Average Convergence Divergence
# ---------------------------------------------------------------------------

def _ema(values: List[float], period: int) -> List[float]:
    """Compute EMA series. Returns list same length as input."""
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1 - alpha) * result[-1])
    return result


def macd(bars, fast: int = 12, slow: int = 26, signal_period: int = 9) -> float:
    """
    MACD histogram normalized by price, mapped to [-1, 1].

    Positive histogram → bullish (trend following)
    Negative histogram → bearish

    Requires at least slow + signal_period bars. Returns 0.0 if insufficient.
    """
    closes = _closes(bars)
    min_bars = slow + signal_period
    if len(closes) < min_bars:
        return 0.0

    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)

    # MACD line = EMA(fast) - EMA(slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]

    # Signal line = EMA of MACD line
    signal_line = _ema(macd_line, signal_period)

    # Histogram = MACD - Signal
    histogram = macd_line[-1] - signal_line[-1]

    # Normalize by current price to make cross-asset comparable
    # 0.5% of price = full signal strength
    current_price = closes[-1]
    if current_price <= 0:
        return 0.0

    normalized = (histogram / current_price) * 100.0  # as percentage
    signal = normalized / 0.5  # 0.5% → ±1.0

    return _clamp(signal)


# ---------------------------------------------------------------------------
# 3. Multi-Timeframe Momentum
# ---------------------------------------------------------------------------

def multi_timeframe_momentum(bars) -> float:
    """
    Blend returns over 1d, 3d, 5d, 10d windows.

    Weights: 1d=0.15, 3d=0.25, 5d=0.30, 10d=0.30
    (Recent gets less weight — noisier.)

    Requires at least 11 bars. Returns 0.0 if insufficient.
    """
    closes = _closes(bars)
    if len(closes) < 11:
        return 0.0

    current = closes[-1]
    windows = [
        (1,  0.15),
        (3,  0.25),
        (5,  0.30),
        (10, 0.30),
    ]

    blended = 0.0
    total_weight = 0.0

    for lookback, weight in windows:
        if len(closes) > lookback:
            past = closes[-1 - lookback]
            if past > 0:
                ret = (current - past) / past * 100.0  # percent return
                blended += ret * weight
                total_weight += weight

    if total_weight <= 0:
        return 0.0

    blended /= total_weight

    # Normalize: 3% blended return = full signal
    signal = blended / 3.0

    return _clamp(signal)


# ---------------------------------------------------------------------------
# 4. Bollinger Band %B
# ---------------------------------------------------------------------------

def bollinger_pct_b(bars, period: int = 20, num_std: float = 2.0) -> float:
    """
    Bollinger %B — position of price within its volatility envelope.

    Mean-reversion signal:
    %B > 1.0 → overbought (negative signal)
    %B < 0.0 → oversold (positive signal)
    %B ≈ 0.5 → near zero

    Requires at least `period` bars. Returns 0.0 if insufficient.
    """
    closes = _closes(bars)
    if len(closes) < period:
        return 0.0

    # Use last `period` closes for SMA and std
    window = closes[-period:]
    sma = sum(window) / period
    variance = sum((c - sma) ** 2 for c in window) / period
    std = math.sqrt(variance) if variance > 0 else 0.0

    if std < 1e-10:
        return 0.0  # flat price → no signal

    upper = sma + num_std * std
    lower = sma - num_std * std
    band_width = upper - lower

    if band_width < 1e-10:
        return 0.0

    pct_b = (closes[-1] - lower) / band_width

    # Mean-reversion mapping
    if pct_b > 1.0:
        # Overbought → bearish
        signal = -(pct_b - 1.0)
    elif pct_b < 0.0:
        # Oversold → bullish
        signal = -pct_b  # pct_b is negative, so -pct_b is positive
    else:
        # Within bands: mild mean-reversion toward midpoint
        signal = (0.5 - pct_b) * 0.5

    return _clamp(signal)


# ---------------------------------------------------------------------------
# 5. Volume-Weighted Momentum
# ---------------------------------------------------------------------------

def volume_weighted_momentum(bars, lookback: int = 10) -> float:
    """
    Price returns weighted by relative volume.

    High-volume moves count more than low-volume moves.
    Falls back to equal-weight if volume data is missing.

    Requires at least 3 bars. Returns 0.0 if insufficient.
    """
    closes = _closes(bars)
    volumes = _volumes(bars)

    # Align — only use last min(len, lookback+1) bars
    n = min(len(closes), len(volumes), lookback + 1)
    if n < 3:
        return 0.0

    closes = closes[-n:]
    volumes = volumes[-n:]

    # Compute returns (n-1 returns from n closes)
    returns = [(closes[i] - closes[i - 1]) / closes[i - 1] * 100.0
               for i in range(1, len(closes))]

    # Get volumes for the return periods (volumes[1:] correspond to returns)
    ret_volumes = volumes[1:]

    # Check if we have usable volume data
    valid_volumes = [v for v in ret_volumes if v is not None and v > 0]
    use_volume = len(valid_volumes) >= len(returns) * 0.5  # at least half have volume

    if use_volume:
        avg_vol = sum(valid_volumes) / len(valid_volumes)
        if avg_vol <= 0:
            use_volume = False

    if use_volume:
        # Volume-weighted returns
        weighted_sum = 0.0
        weight_total = 0.0
        for ret, vol in zip(returns, ret_volumes):
            w = (vol / avg_vol) if (vol is not None and vol > 0) else 1.0
            weighted_sum += ret * w
            weight_total += w
        if weight_total > 0:
            signal_raw = weighted_sum / weight_total
        else:
            signal_raw = sum(returns) / len(returns)
    else:
        # Equal-weight fallback
        signal_raw = sum(returns) / len(returns)

    # Normalize: ±2% average return = full signal
    signal = signal_raw / 2.0

    return _clamp(signal)


# ---------------------------------------------------------------------------
# Composite Momentum — blend all 5 indicators
# ---------------------------------------------------------------------------

def composite_momentum(bars) -> Tuple[float, str]:
    """
    Compute blended momentum signal from all 5 technical indicators.

    Returns:
        (signal_value, source_description)
        signal_value is in [-1, 1].
        source_description summarizes which indicators contributed most.
    """
    # Compute each indicator
    indicators = {
        "rsi":          rsi(bars),
        "macd":         macd(bars),
        "multi_tf":     multi_timeframe_momentum(bars),
        "bollinger":    bollinger_pct_b(bars),
        "vol_weighted": volume_weighted_momentum(bars),
    }

    # Weighted blend
    blended = 0.0
    for name, value in indicators.items():
        weight = MOMENTUM_SUB_WEIGHTS.get(name, 0.20)
        blended += value * weight

    blended = _clamp(blended)

    # Build source description — highlight strongest contributors
    named = [
        (abs(v), name, v) for name, v in indicators.items() if abs(v) > 0.05
    ]
    named.sort(reverse=True)

    if not named:
        source = "Flat — no strong technical signals"
    else:
        parts = []
        labels = {
            "rsi": "RSI", "macd": "MACD", "multi_tf": "Momentum",
            "bollinger": "Bollinger", "vol_weighted": "Vol-Momentum",
        }
        for _, name, val in named[:3]:
            label = labels.get(name, name)
            direction = "bullish" if val > 0 else "bearish"
            parts.append(f"{label} {direction}")
        source = " + ".join(parts)

    return blended, source
