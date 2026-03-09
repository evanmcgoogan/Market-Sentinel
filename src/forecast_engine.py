"""
Deterministic Forecast Engine — always emits predictions for every asset.

All numeric values (direction, magnitude, confidence, expected_return) are
computed from observable signals.  Claude (Haiku) writes narrative ONLY.

Five signal families:
  1. prediction_market  — directional pressure from live prediction markets
  2. whale              — net buy/sell from whale flows
  3. momentum           — EWMA of recent returns
  4. cross_asset        — risk-on/risk-off propagation
  5. news_sentiment     — keyword sentiment on news headlines
"""

import json
import logging
import math
import os
import re
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from technical import composite_momentum

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Reuse from story_generator to avoid circular imports
# ---------------------------------------------------------------------------

OUTLOOK_ASSETS = [
    {"ticker": "SPY",  "name": "S&P 500",         "icon": "\U0001f4c8", "category": "EQUITY"},
    {"ticker": "QQQ",  "name": "Nasdaq 100",       "icon": "\U0001f4bb", "category": "EQUITY"},
    {"ticker": "VIX",  "name": "Volatility Index", "icon": "\u26a1",    "category": "EQUITY",  "inverted": True},
    {"ticker": "GLD",  "name": "Gold",             "icon": "\U0001f947", "category": "COMMODITY"},
    {"ticker": "SLV",  "name": "Silver",           "icon": "\U0001f948", "category": "COMMODITY"},
    {"ticker": "WTI",  "name": "WTI Crude Oil",    "icon": "\U0001f6e2", "category": "COMMODITY"},
    {"ticker": "COPX", "name": "Copper",           "icon": "\U0001f529", "category": "COMMODITY"},
    {"ticker": "DXY",  "name": "US Dollar Index",  "icon": "\U0001f4b5", "category": "FX"},
    {"ticker": "TLT",  "name": "20yr Treasuries",  "icon": "\U0001f3e6", "category": "RATES"},
    {"ticker": "BTC",  "name": "Bitcoin",          "icon": "\u20bf",     "category": "CRYPTO"},
    {"ticker": "ETH",  "name": "Ethereum",         "icon": "\u27e0",     "category": "CRYPTO"},
    {"ticker": "ITA",  "name": "Defense ETF",      "icon": "\U0001f6e1", "category": "SECTOR"},
]

MAGNITUDE_LABELS = {1: "SMALL", 2: "MODERATE", 3: "LARGE", 4: "MAJOR"}
MAGNITUDE_MIDPOINTS = {1: 0.25, 2: 1.0, 3: 2.25, 4: 4.0}  # % typical move

CONFIDENCE_LABELS = {
    (0,  35): "LOW",
    (35, 60): "MEDIUM",
    (60, 80): "HIGH",
    (80, 101): "VERY HIGH",
}


def _confidence_label(score: int) -> str:
    for (lo, hi), label in CONFIDENCE_LABELS.items():
        if lo <= score < hi:
            return label
    return "MEDIUM"


def _magnitude_tier(pct_change: float) -> int:
    a = abs(pct_change)
    if a < 0.5:  return 1
    if a < 1.5:  return 2
    if a < 3.0:  return 3
    return 4


# ---------------------------------------------------------------------------
# Signal weights and constants
# ---------------------------------------------------------------------------

DEFAULT_WEIGHTS: Dict[str, float] = {
    "prediction_market": 0.30,
    "whale":             0.20,
    "momentum":          0.20,
    "cross_asset":       0.15,
    "news_sentiment":    0.15,
}

VOL_LAMBDA = 0.94  # RiskMetrics EWMA decay

# Per-asset weight specialization thresholds
PER_ASSET_MIN_SAMPLES = 15
PER_CATEGORY_MIN_SAMPLES = 10
ASSET_CATEGORY: Dict[str, str] = {a["ticker"]: a["category"] for a in OUTLOOK_ASSETS}

# Prediction market signal tuning constants
_PM_HALF_LIFE_HOURS = 4.0         # market signal halves every 4 hours
_PM_ALERT_HALF_LIFE_HOURS = 2.0   # alerts decay faster — recency matters more
_PM_MIN_VOLUME_FOR_WEIGHT = 10_000  # $10K floor to avoid noise from tiny markets

# Asset-level keyword mappings — which prediction market topics map to each asset
ASSET_KEYWORDS: Dict[str, List[str]] = {
    "SPY":  ["s&p", "stock market", "recession", "fed", "interest rate", "gdp", "economy", "tariff", "trade war", "inflation"],
    "QQQ":  ["nasdaq", "tech", "ai", "semiconductor", "nvidia", "apple", "google", "meta", "amazon", "microsoft"],
    "VIX":  ["volatility", "fear", "crash", "uncertainty", "risk", "panic", "crisis"],
    "GLD":  ["gold", "safe haven", "precious metal", "inflation hedge", "central bank"],
    "SLV":  ["silver", "precious metal", "industrial metal"],
    "WTI":  ["oil", "crude", "opec", "energy", "petroleum", "drilling", "iran", "saudi"],
    "COPX": ["copper", "industrial", "china manufacturing", "construction", "infrastructure"],
    "DXY":  ["dollar", "usd", "currency", "forex", "fed", "interest rate", "treasury"],
    "TLT":  ["bond", "treasury", "yield", "interest rate", "fed", "debt", "deficit"],
    "BTC":  ["bitcoin", "btc", "crypto", "cryptocurrency", "digital asset", "halving"],
    "ETH":  ["ethereum", "eth", "defi", "smart contract", "crypto"],
    "ITA":  ["defense", "military", "war", "nato", "pentagon", "arms", "missile", "conflict", "troops"],
}

# Cross-asset risk correlations: positive = moves with risk-on, negative = moves with risk-off
RISK_CORRELATIONS: Dict[str, float] = {
    "SPY":   1.0,   # benchmark risk-on
    "QQQ":   1.1,   # higher beta risk-on
    "VIX":  -1.2,   # inverse to risk
    "GLD":  -0.4,   # mild safe-haven
    "SLV":   0.2,   # mixed: industrial + safe haven
    "WTI":   0.5,   # risk-on tied to growth
    "COPX":  0.8,   # cyclical
    "DXY":  -0.3,   # mild counter-cyclical
    "TLT":  -0.6,   # flight to safety
    "BTC":   0.7,   # correlated to risk-on lately
    "ETH":   0.8,   # high beta crypto
    "ITA":   0.3,   # mixed: defense spending uncorrelated
}

# News sentiment keywords — bullish/bearish per broad theme
BULLISH_KEYWORDS = [
    "surge", "rally", "soar", "boom", "growth", "bullish", "upbeat",
    "strong", "beat", "record high", "optimism", "recovery", "gain",
    "dovish", "rate cut", "stimulus", "ceasefire", "peace", "deal",
]
BEARISH_KEYWORDS = [
    "crash", "plunge", "collapse", "recession", "bearish", "fear",
    "weak", "miss", "decline", "slump", "crisis", "war", "invasion",
    "hawkish", "rate hike", "default", "sanctions", "escalation", "attack",
    "tariff", "trade war",
]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Driver:
    name: str
    value: float          # signed signal strength (-1 to +1)
    weight: float         # weight applied
    contribution: float   # value * weight
    source: str           # human-readable source
    family: str           # signal family name

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# ForecastEngine
# ---------------------------------------------------------------------------

class ForecastEngine:
    """
    Deterministic multi-asset forecast engine.

    All numeric outputs are computed from observable signals.
    Claude (Haiku) is called ONLY for narrative text — never for numbers.
    """

    HAIKU_MODEL = "claude-haiku-4-5"

    def __init__(self, market_data, db, api_key: str = ""):
        self._md = market_data
        self._db = db
        self._client = None
        key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        if key:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=key)
                logger.info("ForecastEngine: Claude Haiku enabled for narrative")
            except Exception as e:
                logger.warning(f"ForecastEngine: Claude init failed: {e}")

        # Load learned weights or use defaults
        self._weights = dict(DEFAULT_WEIGHTS)
        self._confidence_modifier = 0  # from evaluator cooldown
        self._load_weights()

    def _load_weights(self):
        """Load learned signal weights from DB state (global + per-asset)."""
        try:
            stored = self._db.get_state("forecast_signal_weights", default=None)
            if stored and isinstance(stored, dict):
                weights = stored.get("weights", {})
                if weights and isinstance(weights, dict):
                    for k in self._weights:
                        if k in weights:
                            self._weights[k] = float(weights[k])
                    logger.info(f"ForecastEngine: loaded learned weights: {self._weights}")
                self._confidence_modifier = int(stored.get("confidence_modifier", 0))
        except Exception as e:
            logger.debug(f"ForecastEngine: weight load failed, using defaults: {e}")

        # Load per-asset and per-category weight vectors
        self._per_asset_weights: Dict[str, Dict[str, float]] = {}
        self._per_category_weights: Dict[str, Dict[str, float]] = {}
        try:
            pa_stored = self._db.get_state("forecast_per_asset_weights", default=None)
            if pa_stored and isinstance(pa_stored, dict):
                for ticker, data in pa_stored.get("per_asset", {}).items():
                    if isinstance(data, dict) and "weights" in data:
                        self._per_asset_weights[ticker] = data["weights"]
                for cat, data in pa_stored.get("per_category", {}).items():
                    if isinstance(data, dict) and "weights" in data:
                        self._per_category_weights[cat] = data["weights"]
                n_a = len(self._per_asset_weights)
                n_c = len(self._per_category_weights)
                if n_a or n_c:
                    logger.info(
                        f"ForecastEngine: loaded {n_a} per-asset, "
                        f"{n_c} per-category weight vectors"
                    )
        except Exception as e:
            logger.debug(f"ForecastEngine: per-asset weight load failed: {e}")

    def _load_calibration(self) -> Optional[list]:
        """Load isotonic calibration curve from DB state.
        Returns list of (raw_conf, calibrated_conf) tuples or None."""
        try:
            stored = self._db.get_state("forecast_calibration_curve", default=None)
            if stored and isinstance(stored, dict):
                curve = stored.get("curve")
                if curve and isinstance(curve, list) and len(curve) >= 2:
                    logger.debug(f"ForecastEngine: loaded calibration curve ({len(curve)} points)")
                    return [(float(p[0]), float(p[1])) for p in curve]
        except Exception as e:
            logger.debug(f"ForecastEngine: calibration load failed: {e}")
        return None

    def _apply_calibration(self, raw_confidence: int, calibration_curve: list) -> int:
        """
        Apply isotonic calibration curve to raw confidence score.

        Uses linear interpolation between calibration curve points.
        The curve is a sorted list of (raw, calibrated) pairs.
        """
        if not calibration_curve or len(calibration_curve) < 2:
            return raw_confidence

        raw = float(raw_confidence)

        # Curve is sorted by raw value
        xs = [p[0] for p in calibration_curve]
        ys = [p[1] for p in calibration_curve]

        # Extrapolate below/above curve range
        if raw <= xs[0]:
            return max(15, min(95, int(round(ys[0]))))
        if raw >= xs[-1]:
            return max(15, min(95, int(round(ys[-1]))))

        # Linear interpolation between adjacent points
        for i in range(len(xs) - 1):
            if xs[i] <= raw <= xs[i + 1]:
                span = xs[i + 1] - xs[i]
                if span < 1e-6:
                    return max(15, min(95, int(round(ys[i]))))
                t = (raw - xs[i]) / span
                calibrated = ys[i] + t * (ys[i + 1] - ys[i])
                return max(15, min(95, int(round(calibrated))))

        # Fallback (shouldn't happen)
        return raw_confidence

    # ── Public API ─────────────────────────────────────────────────────

    def generate(self, db) -> Dict[str, Any]:
        """
        Generate a full forecast batch for all assets and both horizons.
        Returns a dict compatible with the existing /api/forecast schema.
        """
        session_id = str(uuid.uuid4())
        now = _utcnow()

        # 0. Load calibration curve (may be None if not yet trained)
        calibration_curve = self._load_calibration()

        # 1. Gather raw signal data
        markets = db.get_top_volume_markets(limit=25, hours=2)
        markets = [m for m in markets if 3 < (m.get("latest_prob") or 50) < 97][:20]
        alerts = db.get_recent_alerts_feed(hours=12, limit=15)
        news = db.get_all_recent_news(hours=12, limit=25)

        # Whale data from cache
        whale_data = None
        try:
            cached_whales = db.get_state("api_whales_cache", default=None)
            if cached_whales and isinstance(cached_whales, dict):
                whale_data = cached_whales.get("data", {})
        except Exception:
            pass

        # 2. Get price history for momentum signals
        price_histories: Dict[str, list] = {}
        for asset in OUTLOOK_ASSETS:
            ticker = asset["ticker"]
            try:
                bars = self._md.get_history(ticker, days=40)
                price_histories[ticker] = bars
            except Exception:
                price_histories[ticker] = []

        # 3. Compute forecasts for each asset
        assets_dict: Dict[str, Dict] = {}
        all_calls: List[Dict] = []  # for persisting to forecast_asset_calls

        # First pass: compute non-cross-asset signals
        # Per-asset weight specialization: swap weight vector per asset
        raw_signals: Dict[str, Dict[str, List[Driver]]] = {}  # ticker -> horizon -> drivers
        for asset in OUTLOOK_ASSETS:
            ticker = asset["ticker"]
            raw_signals[ticker] = {}
            asset_weights = self._get_weights_for_asset(ticker)
            saved_weights = self._weights
            self._weights = asset_weights
            try:
                for horizon in ("24h", "48h"):
                    drivers = []
                    # Signal 1: Prediction market pressure
                    d = self._prediction_market_signal(ticker, markets, alerts)
                    if d: drivers.append(d)
                    # Signal 2: Whale flow
                    d = self._whale_signal(ticker, whale_data)
                    if d: drivers.append(d)
                    # Signal 3: Momentum
                    d = self._momentum_signal(ticker, price_histories.get(ticker, []), horizon)
                    if d: drivers.append(d)
                    # Signal 5: News sentiment
                    d = self._news_signal(ticker, news)
                    if d: drivers.append(d)
                    raw_signals[ticker][horizon] = drivers
            finally:
                self._weights = saved_weights

        # Compute risk-on/risk-off aggregate from SPY + BTC + VIX momentum
        risk_score = self._compute_risk_score(raw_signals)

        # Second pass: add cross-asset signal and compute final forecast
        for asset in OUTLOOK_ASSETS:
            ticker = asset["ticker"]
            meta = asset
            horizons_dict: Dict[str, Any] = {}

            # Per-asset weight specialization for cross-asset signal
            asset_weights = self._get_weights_for_asset(ticker)
            saved_weights = self._weights
            self._weights = asset_weights
            try:
              for horizon in ("24h", "48h"):
                drivers = list(raw_signals[ticker][horizon])

                # Signal 4: Cross-asset risk propagation
                d = self._cross_asset_signal(ticker, risk_score)
                if d: drivers.append(d)

                # Compute net pressure
                net_pressure = sum(d.contribution for d in drivers)

                # Direction — NEVER "—"
                direction = "UP" if net_pressure >= 0 else "DOWN"

                # Adjust for inverted assets (VIX: UP pressure → higher VIX → risk-off)
                # The direction here refers to the asset's price direction, which is correct

                # Magnitude from EWMA volatility
                vol = self._ewma_volatility(price_histories.get(ticker, []))
                magnitude_tier = self._magnitude_from_pressure(abs(net_pressure), vol)
                magnitude_label = MAGNITUDE_LABELS.get(magnitude_tier, "SMALL")

                # Confidence from signal agreement + modifier
                confidence = self._compute_confidence(drivers, net_pressure)
                confidence = max(15, min(95, confidence + self._confidence_modifier))

                # Apply isotonic calibration if available
                if calibration_curve:
                    confidence = self._apply_calibration(confidence, calibration_curve)

                confidence_lbl = _confidence_label(confidence)

                # Horizon scaling: 48h gets slightly lower confidence, higher magnitude potential
                if horizon == "48h":
                    confidence = max(15, confidence - 5)
                    confidence_lbl = _confidence_label(confidence)

                # Expected return
                dir_sign = 1.0 if direction == "UP" else -1.0
                mid = MAGNITUDE_MIDPOINTS.get(magnitude_tier, 0.25)
                expected_return = round(dir_sign * mid * (confidence / 100.0), 4)

                # Probabilities
                p_direction = confidence / 100.0
                p_flat = 0.10  # baseline uncertainty
                if direction == "UP":
                    p_up = round(p_direction * (1 - p_flat), 3)
                    p_down = round((1 - p_direction) * (1 - p_flat), 3)
                else:
                    p_down = round(p_direction * (1 - p_flat), 3)
                    p_up = round((1 - p_direction) * (1 - p_flat), 3)

                # Build driver dicts for output (top 3 by |contribution|)
                sorted_drivers = sorted(drivers, key=lambda d: abs(d.contribution), reverse=True)
                driver_names = [d.name for d in sorted_drivers[:3]]
                driver_dicts = [d.to_dict() for d in sorted_drivers[:5]]

                pred = {
                    "direction": direction,
                    "magnitude_score": magnitude_tier,
                    "magnitude_label": magnitude_label,
                    "confidence": confidence,
                    "confidence_label": confidence_lbl,
                    "expected_return": expected_return,
                    "p_up": p_up,
                    "p_down": p_down,
                    "p_flat": round(p_flat, 3),
                    "drivers": driver_names,
                    "driver_details": driver_dicts,
                }
                horizons_dict[horizon] = pred

                # Collect for DB persistence
                all_calls.append({
                    "ticker": ticker,
                    "horizon": horizon,
                    "direction": direction,
                    "magnitude": magnitude_label,
                    "confidence": confidence,
                    "expected_return": expected_return,
                    "p_up": p_up,
                    "p_down": p_down,
                    "p_flat": round(p_flat, 3),
                    "drivers": driver_dicts,
                })
            finally:
              self._weights = saved_weights

            horizons_dict["ticker"] = ticker
            horizons_dict["name"] = meta.get("name", ticker)
            horizons_dict["category"] = meta.get("category", "OTHER")
            horizons_dict["inverted"] = meta.get("inverted", False)
            assets_dict[ticker] = horizons_dict

        # 4. Determine market regime
        regime = self._determine_regime(assets_dict)

        # 5. Generate narrative (Claude Haiku — optional)
        summary, themes, note = self._generate_narrative(
            assets_dict, regime, markets, alerts, news
        )

        generated_at = now.isoformat()

        result = {
            "session_id": session_id,
            "generated_at": generated_at,
            "market_regime": regime,
            "outlook_summary": summary,
            "dominant_themes": themes,
            "generated_note": note,
            "assets": assets_dict,
            "asset_order": [a["ticker"] for a in OUTLOOK_ASSETS],
        }

        # 6. Persist to DB
        try:
            db.save_outlook_prediction(
                session_id=session_id,
                generated_at=generated_at,
                market_regime=regime,
                outlook_summary=summary,
                dominant_themes_json=json.dumps(themes),
                assets_json=json.dumps(assets_dict),
            )
        except Exception as e:
            logger.warning(f"ForecastEngine: failed to persist prediction: {e}")

        try:
            db.save_forecast_calls(session_id, generated_at, all_calls)
        except Exception as e:
            logger.warning(f"ForecastEngine: failed to persist forecast calls: {e}")

        return result

    # ── Signal Computers ───────────────────────────────────────────────

    def _prediction_market_signal(
        self, ticker: str, markets: list, alerts: list
    ) -> Optional[Driver]:
        """
        Scan prediction markets for signals relevant to this asset.

        Weights each market by:
          - sqrt(volume_24h / floor)  — bigger markets carry more information
          - exp(-age / half_life)     — recent data dominates stale data
          - keyword relevance         — how many keywords match

        Weights each alert by:
          - min(|delta|/5, 2.0)       — larger moves carry more signal
          - exp(-age / half_life)     — 2h half-life (faster decay than markets)
          - keyword relevance
        """
        keywords = ASSET_KEYWORDS.get(ticker, [])
        if not keywords:
            return None

        now = _utcnow()
        weighted_bullish = 0.0
        weighted_bearish = 0.0
        total_weight = 0.0
        top_market_name = ""
        top_market_weight = 0.0
        top_market_volume = 0.0
        top_market_delta = 0.0

        # --- Market signals (probability levels) ---
        for m in markets:
            name = (m.get("market_name") or "").lower()
            prob = m.get("latest_prob") or 50
            relevance = sum(1 for kw in keywords if kw.lower() in name)
            if relevance == 0:
                continue

            # Volume weight: sqrt(vol / floor); default 1.0 if missing
            vol = m.get("volume_24h") or 0
            if vol > 0:
                vol_weight = math.sqrt(max(vol, _PM_MIN_VOLUME_FOR_WEIGHT) / _PM_MIN_VOLUME_FOR_WEIGHT)
            else:
                vol_weight = 1.0

            # Time decay: exp(-0.693 * age_hours / half_life)
            age_hours = self._hours_since(m.get("latest_ts"), now)
            time_weight = math.exp(-0.693 * age_hours / _PM_HALF_LIFE_HOURS)

            composite_weight = relevance * vol_weight * time_weight

            # High-prob bullish-sounding markets push UP, low-prob push DOWN
            if prob > 55:
                weighted_bullish += composite_weight * (prob - 50) / 50
            elif prob < 45:
                weighted_bearish += composite_weight * (50 - prob) / 50
            total_weight += composite_weight

            # Track top contributing market for source description
            if composite_weight > top_market_weight:
                top_market_weight = composite_weight
                top_market_name = m.get("market_name") or ""
                top_market_volume = vol
                top_market_delta = prob - 50

        # --- Alert signals (probability changes) ---
        top_alert_delta = 0.0
        for a in alerts:
            name = (a.get("market_name") or "").lower()
            new_p = a.get("new_probability") or 50
            old_p = a.get("old_probability") or 50
            relevance = sum(1 for kw in keywords if kw.lower() in name)
            if relevance == 0:
                continue

            delta = new_p - old_p
            if abs(delta) < 0.5:
                continue  # noise filter

            # Magnitude weight: larger moves carry more signal
            mag_weight = min(abs(delta) / 5.0, 2.0)

            # Time decay (faster for alerts — recency matters more)
            age_hours = self._hours_since(a.get("timestamp"), now)
            time_weight = math.exp(-0.693 * age_hours / _PM_ALERT_HALF_LIFE_HOURS)

            composite_weight = relevance * mag_weight * time_weight

            if delta > 0:
                weighted_bullish += composite_weight
            else:
                weighted_bearish += composite_weight
            total_weight += composite_weight

            if abs(delta) > abs(top_alert_delta):
                top_alert_delta = delta

        if total_weight < 0.01:
            return None

        # Normalize to [-1, 1]
        raw = (weighted_bullish - weighted_bearish) / total_weight
        value = max(-1.0, min(1.0, raw))
        weight = self._weights.get("prediction_market", 0.30)

        source = self._pm_source_description(
            top_market_name, top_market_volume, top_alert_delta, total_weight
        )

        return Driver(
            name=f"Prediction market {'bullish' if value > 0 else 'bearish'} on {ticker}",
            value=round(value, 3),
            weight=weight,
            contribution=round(value * weight, 4),
            source=source,
            family="prediction_market",
        )

    def _whale_signal(self, ticker: str, whale_data: Optional[dict]) -> Optional[Driver]:
        """Net buy/sell pressure from whale flows relevant to this asset."""
        if not whale_data:
            return None

        keywords = ASSET_KEYWORDS.get(ticker, [])
        flows = whale_data.get("market_flows") or []
        if not flows and not keywords:
            return None

        net_usd = 0.0
        total_usd = 0.0

        for flow in flows:
            title = (flow.get("title") or flow.get("market_title") or "").lower()
            relevance = sum(1 for kw in keywords if kw.lower() in title)
            if relevance == 0:
                continue
            buy_vol = flow.get("buy_volume_usd") or flow.get("buy_volume") or 0
            sell_vol = flow.get("sell_volume_usd") or flow.get("sell_volume") or 0
            net_usd += (buy_vol - sell_vol) * relevance
            total_usd += (buy_vol + sell_vol) * relevance

        if total_usd < 1000:  # negligible
            return None

        # Normalize: $100K net flow → +-0.5 signal
        value = max(-1.0, min(1.0, net_usd / 200_000))
        weight = self._weights.get("whale", 0.20)

        return Driver(
            name=f"Whale {'buying' if value > 0 else 'selling'} pressure",
            value=round(value, 3),
            weight=weight,
            contribution=round(value * weight, 4),
            source=f"${abs(net_usd):,.0f} net flow",
            family="whale",
        )

    def _momentum_signal(
        self, ticker: str, bars: list, horizon: str
    ) -> Optional[Driver]:
        """
        Composite momentum from 5 technical indicators:
        RSI(14), MACD(12,26,9), multi-TF momentum, Bollinger %B,
        volume-weighted momentum.
        """
        if len(bars) < 3:
            return Driver(
                name=f"Momentum unavailable ({ticker})",
                value=0.0,
                weight=self._weights.get("momentum", 0.20),
                contribution=0.0,
                source="insufficient data",
                family="momentum",
            )

        value, source_desc = composite_momentum(bars)

        # 48h gets slightly amplified momentum — trends persist
        if horizon == "48h":
            value = max(-1.0, min(1.0, value * 1.2))

        weight = self._weights.get("momentum", 0.20)

        return Driver(
            name=f"{ticker} {'uptrend' if value > 0 else 'downtrend'} momentum",
            value=round(value, 3),
            weight=weight,
            contribution=round(value * weight, 4),
            source=source_desc,
            family="momentum",
        )

    def _cross_asset_signal(self, ticker: str, risk_score: float) -> Optional[Driver]:
        """Propagate risk-on/risk-off sentiment to this asset."""
        beta = RISK_CORRELATIONS.get(ticker, 0.0)
        if abs(beta) < 0.01:
            return None

        # risk_score > 0 = risk-on, < 0 = risk-off
        value = max(-1.0, min(1.0, risk_score * beta * 0.5))
        weight = self._weights.get("cross_asset", 0.15)

        regime = "risk-on" if risk_score > 0 else "risk-off"
        return Driver(
            name=f"Cross-asset {regime} signal",
            value=round(value, 3),
            weight=weight,
            contribution=round(value * weight, 4),
            source=f"risk_score={risk_score:+.2f}, beta={beta:+.1f}",
            family="cross_asset",
        )

    def _news_signal(self, ticker: str, news: list) -> Optional[Driver]:
        """Keyword-based sentiment from recent news headlines."""
        keywords = ASSET_KEYWORDS.get(ticker, [])
        if not news:
            return None

        bullish = 0
        bearish = 0
        relevant_count = 0

        for article in news:
            title = (article.get("title") or "").lower()
            # Check if article is relevant to this asset
            is_relevant = any(kw.lower() in title for kw in keywords)
            if not is_relevant:
                continue
            relevant_count += 1

            for bw in BULLISH_KEYWORDS:
                if bw in title:
                    bullish += 1
            for bw in BEARISH_KEYWORDS:
                if bw in title:
                    bearish += 1

        if relevant_count == 0:
            return None

        raw = (bullish - bearish) / max(relevant_count, 1)
        value = max(-1.0, min(1.0, raw * 0.5))
        weight = self._weights.get("news_sentiment", 0.15)

        return Driver(
            name=f"News {'positive' if value > 0 else 'negative'} for {ticker}",
            value=round(value, 3),
            weight=weight,
            contribution=round(value * weight, 4),
            source=f"{relevant_count} relevant articles",
            family="news_sentiment",
        )

    # ── Prediction Market Helpers ──────────────────────────────────────

    @staticmethod
    def _hours_since(timestamp_str: Optional[str], now: datetime) -> float:
        """Parse ISO timestamp, return hours elapsed. Defaults to 24h on failure."""
        if not timestamp_str:
            return 24.0
        try:
            ts = timestamp_str
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            dt = datetime.fromisoformat(ts)
            dt = dt.replace(tzinfo=None)  # match naive UTC
            delta = (now - dt).total_seconds() / 3600.0
            return max(0.0, delta)
        except Exception:
            return 24.0

    @staticmethod
    def _pm_source_description(
        top_market: str, volume: float, alert_delta: float, total_weight: float
    ) -> str:
        """Build rich source string for prediction market driver."""
        parts = []
        if top_market:
            name_short = top_market[:60] + ("…" if len(top_market) > 60 else "")
            parts.append(name_short)
        if volume > 0:
            if volume >= 1_000_000:
                parts.append(f"${volume / 1_000_000:.1f}M vol")
            elif volume >= 1_000:
                parts.append(f"${volume / 1_000:.0f}K vol")
        if abs(alert_delta) >= 1:
            direction = "up" if alert_delta > 0 else "down"
            parts.append(f"{abs(alert_delta):.0f}pp {direction}")
        parts.append(f"wt={total_weight:.1f}")
        return " | ".join(parts) if parts else "prediction markets"

    # ── Per-Asset Weight Resolution ────────────────────────────────────

    def _get_weights_for_asset(self, ticker: str) -> Dict[str, float]:
        """
        Resolve weight vector for a specific asset.
        Three-tier fallback: per-asset → per-category → global.
        """
        # Tier 1: per-asset weights
        if ticker in self._per_asset_weights:
            return self._per_asset_weights[ticker]

        # Tier 2: per-category weights
        category = ASSET_CATEGORY.get(ticker)
        if category and category in self._per_category_weights:
            return self._per_category_weights[category]

        # Tier 3: global weights
        return self._weights

    # ── Helpers ────────────────────────────────────────────────────────

    def _compute_risk_score(self, raw_signals: dict) -> float:
        """Aggregate risk-on/off score from SPY, BTC, VIX momentum signals."""
        score = 0.0
        count = 0
        for ticker, beta in [("SPY", 1.0), ("BTC", 0.5), ("VIX", -0.8)]:
            signals = raw_signals.get(ticker, {})
            for horizon_drivers in signals.values():
                for d in horizon_drivers:
                    if d.family == "momentum":
                        score += d.value * beta
                        count += 1
                        break  # one momentum signal per horizon
                break  # only use 24h horizon
        return max(-1.0, min(1.0, score / max(count, 1)))

    def _ewma_volatility(self, bars: list) -> float:
        """EWMA volatility estimate from daily bars. Returns annualized vol %."""
        closes = [b.close for b in bars if b.close > 0]
        if len(closes) < 3:
            return 1.5  # default moderate vol

        returns = [(closes[i] - closes[i-1]) / closes[i-1] * 100
                    for i in range(1, len(closes))]

        # EWMA variance
        var = returns[0] ** 2
        for r in returns[1:]:
            var = VOL_LAMBDA * var + (1 - VOL_LAMBDA) * r ** 2

        daily_vol = math.sqrt(max(var, 0.001))
        return daily_vol

    def _magnitude_from_pressure(self, abs_pressure: float, daily_vol: float) -> int:
        """Convert absolute net pressure to magnitude tier, scaled by volatility."""
        # Scale: higher vol → higher magnitude thresholds
        vol_scale = max(0.5, daily_vol / 1.0)
        scaled = abs_pressure / vol_scale

        if scaled < 0.05:   return 1  # SMALL
        if scaled < 0.15:   return 2  # MODERATE
        if scaled < 0.30:   return 3  # LARGE
        return 4                       # MAJOR

    def _compute_confidence(self, drivers: list, net_pressure: float) -> int:
        """
        Confidence based on signal agreement and strength.
        More drivers agreeing on direction → higher confidence.
        """
        if not drivers:
            return 20  # baseline low conviction

        # Count signals agreeing with net direction
        direction = 1 if net_pressure >= 0 else -1
        agreeing = sum(1 for d in drivers if (d.value > 0) == (direction > 0) and abs(d.value) > 0.01)
        total = len([d for d in drivers if abs(d.value) > 0.01])

        if total == 0:
            return 20

        agreement_ratio = agreeing / total  # 0 to 1

        # Base confidence from agreement
        base = 20 + int(agreement_ratio * 50)  # 20 to 70

        # Boost from signal strength
        avg_strength = sum(abs(d.value) for d in drivers) / len(drivers)
        strength_boost = int(avg_strength * 25)  # 0 to 25

        return min(95, base + strength_boost)

    def _determine_regime(self, assets: dict) -> str:
        """Deterministic market regime from asset directions."""
        risk_on_up = 0
        risk_on_down = 0

        for ticker, beta in [("SPY", 1.0), ("QQQ", 1.0), ("BTC", 0.5),
                              ("VIX", -1.0), ("GLD", -0.5), ("TLT", -0.5)]:
            asset = assets.get(ticker, {})
            pred_24h = asset.get("24h", {})
            direction = pred_24h.get("direction", "UP")
            conf = pred_24h.get("confidence", 50) / 100

            if direction == "UP":
                risk_on_up += beta * conf
            else:
                risk_on_down += beta * conf

        net = risk_on_up - risk_on_down
        if net > 0.5:
            return "RISK-ON"
        elif net < -0.5:
            return "RISK-OFF"
        elif abs(net) < 0.2:
            return "NEUTRAL"
        else:
            return "MIXED"

    # ── Narrative (Claude Haiku — optional) ────────────────────────────

    def _generate_narrative(
        self, assets: dict, regime: str, markets: list, alerts: list, news: list
    ) -> Tuple[str, List[str], str]:
        """
        Generate outlook_summary, dominant_themes, generated_note.
        Uses Claude Haiku if available, otherwise template strings.
        """
        if not self._client:
            return self._template_narrative(assets, regime)

        # Build a compact context for Claude
        asset_lines = []
        for a in OUTLOOK_ASSETS:
            t = a["ticker"]
            data = assets.get(t, {})
            p24 = data.get("24h", {})
            p48 = data.get("48h", {})
            asset_lines.append(
                f"{t}: 24h {p24.get('direction','?')} "
                f"(mag={p24.get('magnitude_label','?')}, conf={p24.get('confidence',0)}%) "
                f"| 48h {p48.get('direction','?')} "
                f"(mag={p48.get('magnitude_label','?')}, conf={p48.get('confidence',0)}%)"
            )

        top_markets = "\n".join(
            f"  - \"{m.get('market_name','')}\" {m.get('latest_prob',50):.0f}%"
            for m in (markets or [])[:8]
        ) or "  (none)"

        top_news = "\n".join(
            f"  - \"{n.get('title','')}\""
            for n in (news or [])[:8]
        ) or "  (none)"

        prompt = f"""Given these deterministic asset forecasts and the current market signals, write:
1. outlook_summary: 2-3 sentences — the dominant macro narrative, what it means, biggest risk
2. dominant_themes: list of 4-6 short theme strings
3. generated_note: 1 sentence — the #1 signal that stands out most

FORECASTS:
{chr(10).join(asset_lines)}

REGIME: {regime}

TOP PREDICTION MARKETS:
{top_markets}

RECENT NEWS:
{top_news}

Return ONLY valid compact JSON:
{{"outlook_summary":"...","dominant_themes":["..."],"generated_note":"..."}}"""

        try:
            msg = self._client.messages.create(
                model=self.HAIKU_MODEL,
                max_tokens=500,
                system=(
                    "You are a senior macro strategist writing a brief market narrative. "
                    "Be direct, specific, grounded in the signals. No generic filler. "
                    "Return only valid compact JSON."
                ),
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text.strip()
            raw = re.sub(r'^```(?:json)?\s*', '', raw)
            raw = re.sub(r'\s*```\s*$', '', raw.strip())
            data = json.loads(raw)
            return (
                data.get("outlook_summary", ""),
                data.get("dominant_themes", []),
                data.get("generated_note", ""),
            )
        except Exception as e:
            logger.warning(f"ForecastEngine: Claude narrative failed: {e}")
            return self._template_narrative(assets, regime)

    def _template_narrative(
        self, assets: dict, regime: str
    ) -> Tuple[str, List[str], str]:
        """Fallback narrative without Claude."""
        up_assets = []
        down_assets = []
        for a in OUTLOOK_ASSETS:
            t = a["ticker"]
            d = assets.get(t, {}).get("24h", {}).get("direction", "UP")
            if d == "UP":
                up_assets.append(t)
            else:
                down_assets.append(t)

        summary = (
            f"Market regime is {regime}. "
            f"{len(up_assets)} assets forecast higher ({', '.join(up_assets[:4])}), "
            f"{len(down_assets)} lower ({', '.join(down_assets[:4])}). "
            "Monitor cross-asset correlations for regime shifts."
        )

        themes = []
        if regime in ("RISK-ON", "MIXED"):
            themes.append("Risk appetite improving")
        if regime in ("RISK-OFF", "MIXED"):
            themes.append("Defensive positioning")
        if "VIX" in down_assets:
            themes.append("Volatility compression")
        elif "VIX" in up_assets:
            themes.append("Rising uncertainty")
        if "GLD" in up_assets:
            themes.append("Safe haven demand")
        if "BTC" in up_assets:
            themes.append("Crypto momentum")
        themes = themes[:6] or ["Monitoring signals"]

        note = f"Regime is {regime} based on cross-asset signal analysis."

        return summary, themes, note

    # ── Fallback for cold start ────────────────────────────────────────

    def fallback(self, reason: str = "") -> Dict[str, Any]:
        """Return a skeleton structure when forecast can't be computed."""
        if not reason:
            reason = "Generating forecast — computing signals across all assets."
        assets = {}
        for a in OUTLOOK_ASSETS:
            assets[a["ticker"]] = {
                "ticker": a["ticker"], "name": a["name"],
                "category": a["category"], "inverted": a.get("inverted", False),
                "24h": {
                    "direction": "UP", "magnitude_score": 1, "magnitude_label": "SMALL",
                    "confidence": 20, "confidence_label": "LOW",
                    "expected_return": 0.0, "p_up": 0.50, "p_down": 0.40, "p_flat": 0.10,
                    "drivers": ["Baseline forecast"], "driver_details": [],
                },
                "48h": {
                    "direction": "UP", "magnitude_score": 1, "magnitude_label": "SMALL",
                    "confidence": 15, "confidence_label": "LOW",
                    "expected_return": 0.0, "p_up": 0.50, "p_down": 0.40, "p_flat": 0.10,
                    "drivers": ["Baseline forecast"], "driver_details": [],
                },
            }
        return {
            "outlook_summary": reason,
            "market_regime": "NEUTRAL",
            "dominant_themes": [],
            "generated_note": "",
            "assets": assets,
            "asset_order": [a["ticker"] for a in OUTLOOK_ASSETS],
            "generated_at": _utcnow().isoformat(),
        }
