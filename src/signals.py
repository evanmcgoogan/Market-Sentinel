"""
Signal detection heuristics.
Simple, transparent rules for detecting early market movements.
No ML - just straightforward thresholds you can understand and tune.

Signals detected:
  1. Price Velocity        — sudden probability changes
  2. Volume Shock          — abrupt volume spikes
  3. Thin Liquidity Jump   — large moves on small volume
  4. Cross-Market Divergence — platform disagreement
  5. Late-Stage Sensitivity — boost signals near resolution
  6. Odd-Hour Activity     — volume spikes at suspicious times (NEW)
  7. Acceleration          — velocity-of-velocity, second derivative (NEW)
  8. Order Book Imbalance  — one-sided stacking / spread tightening (NEW)
  9. No-News Move          — significant move with zero news coverage (NEW)
 10. Whale Activity        — smart money moving into positions (NEW)
 11. Multi-Signal Correlation — coincidence bonus when signals stack (NEW)
"""

import re
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Set, Tuple

from models import Market, Signal, Alert, MarketPair, utcnow, ensure_aware
from config import SignalThresholds
from database import Database


logger = logging.getLogger(__name__)


class SignalDetector:
    """
    Detects abnormal market movements using simple heuristics.
    Extended with 6 new signal types for deeper anomaly detection.
    """

    def __init__(
        self,
        config: SignalThresholds,
        db: Database,
        news_monitor=None,
        whale_tracker=None,
        orderbook_analyzer=None,
    ):
        self.config = config
        self.db = db
        self.news_monitor = news_monitor
        self.whale_tracker = whale_tracker
        self.orderbook_analyzer = orderbook_analyzer

    def detect_signals(
        self,
        market: Market,
        paired_market: Optional[Market] = None,
    ) -> List[Signal]:
        """
        Analyze a market and detect all active signals.
        Returns list of detected signals with their strengths.
        """
        signals = []

        # === Original signals ===

        # 1. Price Velocity
        velocity_signal = self._detect_price_velocity(market)
        if velocity_signal:
            signals.append(velocity_signal)

        # 2. Volume Shock
        volume_signal = self._detect_volume_shock(market)
        if volume_signal:
            signals.append(volume_signal)

        # 3. Thin Liquidity Jump
        thin_signal = self._detect_thin_liquidity_jump(market)
        if thin_signal:
            signals.append(thin_signal)

        # 4. Cross-Market Divergence
        if paired_market:
            divergence_signal = self._detect_cross_market_divergence(market, paired_market)
            if divergence_signal:
                signals.append(divergence_signal)

        # === New signals ===

        # 6. Odd-Hour Activity
        odd_hour_signal = self._detect_odd_hour_activity(market)
        if odd_hour_signal:
            signals.append(odd_hour_signal)

        # 7. Acceleration (velocity of velocity)
        accel_signal = self._detect_acceleration(market)
        if accel_signal:
            signals.append(accel_signal)

        # 8. Order Book Imbalance
        if self.orderbook_analyzer:
            ob_signal = self._detect_orderbook_imbalance(market)
            if ob_signal:
                signals.append(ob_signal)

        # 9. No-News Move
        if self.news_monitor:
            no_news_signal = self._detect_no_news_move(market)
            if no_news_signal:
                signals.append(no_news_signal)

        # 10. Whale Activity
        if self.whale_tracker:
            whale_signal = self._detect_whale_activity(market)
            if whale_signal:
                signals.append(whale_signal)

        return signals

    # ==================== Original Signals ====================

    def _detect_price_velocity(self, market: Market) -> Optional[Signal]:
        """Detect sudden probability changes over short time windows."""
        window_minutes = self.config.price_velocity_time_window_minutes
        min_change = self.config.price_velocity_min_change

        snapshots = self.db.get_recent_snapshots(
            market.platform_str,
            market.market_id,
            minutes=window_minutes,
        )

        if len(snapshots) < 2:
            return None

        oldest = snapshots[0]
        current_prob = market.probability
        old_prob = oldest["probability"]
        change = abs(current_prob - old_prob)

        if change < min_change:
            return None

        try:
            oldest_time = datetime.fromisoformat(oldest["timestamp"])
            oldest_time = ensure_aware(oldest_time)
            time_delta = (utcnow() - oldest_time).total_seconds() / 60
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse snapshot timestamp: {e}")
            time_delta = float(window_minutes)

        strength = min(40, (change / min_change) * 20)
        direction = "↑" if current_prob > old_prob else "↓"

        return Signal(
            signal_type="price_velocity",
            description=f"Sudden move {direction}{change:.1f}pp in {time_delta:.0f}m",
            strength=strength,
            data={
                "old_probability": old_prob,
                "new_probability": current_prob,
                "change": change,
                "time_delta_minutes": time_delta,
            },
        )

    def _detect_volume_shock(self, market: Market) -> Optional[Signal]:
        """Detect abrupt volume spikes relative to recent baseline."""
        multiplier = self.config.volume_shock_multiplier

        if market.volume_24h <= 0:
            return None

        baseline = self.db.get_baseline_volume(
            market.platform_str,
            market.market_id,
            hours=self.config.volume_baseline_hours,
        )

        if baseline is None or baseline <= 0:
            return None

        ratio = market.volume_24h / baseline

        if ratio < multiplier:
            return None

        strength = min(30, (ratio / multiplier) * 15)

        return Signal(
            signal_type="volume_shock",
            description=f"Volume {ratio:.1f}x normal",
            strength=strength,
            data={
                "current_volume": market.volume_24h,
                "baseline_volume": baseline,
                "ratio": ratio,
            },
        )

    def _detect_thin_liquidity_jump(self, market: Market) -> Optional[Signal]:
        """Detect large price moves on relatively small volume."""
        max_volume = self.config.thin_liquidity_max_volume
        min_price_change = self.config.thin_liquidity_min_price_change

        is_thin = market.volume_24h < max_volume and market.liquidity < max_volume
        if not is_thin:
            return None

        snapshots = self.db.get_recent_snapshots(
            market.platform_str,
            market.market_id,
            minutes=60,
        )

        if len(snapshots) < 2:
            return None

        oldest = snapshots[0]
        change = abs(market.probability - oldest["probability"])

        if change < min_price_change:
            return None

        thin_factor = max_volume / max(market.volume_24h, 100)
        strength = min(25, change * thin_factor)

        return Signal(
            signal_type="thin_liquidity_jump",
            description=f"Big move ({change:.1f}pp) on thin market",
            strength=strength,
            data={
                "price_change": change,
                "volume_24h": market.volume_24h,
                "liquidity": market.liquidity,
            },
        )

    def _detect_cross_market_divergence(
        self,
        market: Market,
        paired_market: Market,
    ) -> Optional[Signal]:
        """Detect when Polymarket and Kalshi disagree significantly."""
        threshold = self.config.cross_market_divergence_threshold
        divergence = abs(market.probability - paired_market.probability)

        if divergence < threshold:
            return None

        if market.probability > paired_market.probability:
            leader = market.platform_str
        else:
            leader = paired_market.platform_str

        strength = min(30, (divergence / threshold) * 15)

        return Signal(
            signal_type="cross_market_divergence",
            description=f"{leader.title()} leading ({divergence:.1f}pp gap)",
            strength=strength,
            data={
                "divergence": divergence,
                "leader": leader,
                "market_prob": market.probability,
                "paired_prob": paired_market.probability,
            },
        )

    # ==================== New Signals ====================

    def _detect_odd_hour_activity(self, market: Market) -> Optional[Signal]:
        """
        Detect volume spikes at unusual hours.
        3am EST activity on a political market? That smells like signal.
        Builds per-hour baselines and flags deviations.
        """
        now = utcnow()
        current_hour = now.hour

        if market.volume_24h <= 0:
            return None

        # Update the hourly baseline with current observation
        self.db.update_hourly_volume_baseline(
            platform=market.platform_str,
            market_id=market.market_id,
            hour_utc=current_hour,
            volume=market.volume_24h,
        )

        # Get baseline for this hour
        baseline = self.db.get_hourly_volume_baseline(
            platform=market.platform_str,
            market_id=market.market_id,
            hour_utc=current_hour,
        )

        if not baseline or baseline["sample_count"] < self.config.odd_hour_min_baseline_samples:
            return None

        avg_volume = baseline["avg_volume"]
        if avg_volume <= 0:
            return None

        ratio = market.volume_24h / avg_volume
        multiplier = self.config.odd_hour_volume_multiplier

        if ratio < multiplier:
            return None

        # Extra strength if this is an off-peak hour
        is_off_peak = current_hour in self.config.off_peak_hours_utc
        strength = min(25, (ratio / multiplier) * 12)

        if is_off_peak:
            strength *= self.config.off_peak_bonus_multiplier
            strength = min(35, strength)

        time_label = f"{current_hour:02d}:00 UTC"
        peak_note = " (off-peak)" if is_off_peak else ""

        return Signal(
            signal_type="odd_hour_activity",
            description=f"Unusual activity at {time_label}{peak_note} ({ratio:.1f}x normal)",
            strength=strength,
            data={
                "hour_utc": current_hour,
                "volume_ratio": ratio,
                "is_off_peak": is_off_peak,
                "baseline_volume": avg_volume,
                "current_volume": market.volume_24h,
            },
        )

    def _detect_acceleration(self, market: Market) -> Optional[Signal]:
        """
        Detect acceleration — the second derivative of price movement.
        Price velocity tells you "it moved fast."
        Acceleration tells you "it's moving FASTER" — the momentum is increasing.
        This catches the beginning of a cascade/avalanche before it peaks.
        """
        window = self.config.acceleration_window_minutes

        snapshots = self.db.get_recent_snapshots(
            market.platform_str,
            market.market_id,
            minutes=window,
        )

        if len(snapshots) < 3:
            return None

        # Calculate velocities between consecutive snapshots
        velocities = []
        for i in range(1, len(snapshots)):
            try:
                t1 = datetime.fromisoformat(snapshots[i - 1]["timestamp"])
                t2 = datetime.fromisoformat(snapshots[i]["timestamp"])
                t1 = ensure_aware(t1)
                t2 = ensure_aware(t2)
                dt = (t2 - t1).total_seconds() / 60  # minutes
                if dt <= 0:
                    continue
                dp = snapshots[i]["probability"] - snapshots[i - 1]["probability"]
                velocity = dp / dt  # pp per minute
                velocities.append(velocity)
            except (ValueError, KeyError):
                continue

        if len(velocities) < 2:
            return None

        # Calculate acceleration (change in velocity)
        accelerations = []
        for i in range(1, len(velocities)):
            accel = velocities[i] - velocities[i - 1]
            accelerations.append(accel)

        if not accelerations:
            return None

        latest_accel = accelerations[-1]
        abs_accel = abs(latest_accel)
        threshold = self.config.acceleration_min_threshold

        if abs_accel < threshold:
            return None

        # Check that latest velocity is meaningful (not oscillating around zero)
        latest_velocity = velocities[-1]
        if abs(latest_velocity) < 0.1:
            return None

        # Aligned = velocity and acceleration in same direction = building momentum
        aligned = (latest_velocity > 0 and latest_accel > 0) or \
                  (latest_velocity < 0 and latest_accel < 0)

        strength = min(30, (abs_accel / threshold) * 15)
        if aligned:
            strength *= 1.3
            strength = min(35, strength)

        direction = "↑ accelerating" if latest_accel > 0 else "↓ accelerating"

        return Signal(
            signal_type="acceleration",
            description=f"Price {direction} ({abs_accel:.2f} pp/min²)",
            strength=strength,
            data={
                "acceleration": latest_accel,
                "latest_velocity": latest_velocity,
                "aligned": aligned,
                "velocity_history": velocities[-5:],
            },
        )

    def _detect_orderbook_imbalance(self, market: Market) -> Optional[Signal]:
        """
        Detect order book anomalies: one-sided stacking, spread tightening.
        Heavy bids with light asks = someone knows price is going up.
        """
        if not self.orderbook_analyzer:
            return None

        analysis = self.orderbook_analyzer.detect_imbalance(
            platform=market.platform_str,
            market_id=market.market_id,
        )

        if not analysis or not analysis.get("has_imbalance"):
            return None

        reasons = analysis.get("reasons", [])
        ratio = analysis.get("bid_ask_ratio", 1.0)

        imbalance_factor = max(ratio, 1 / ratio if ratio > 0 else 1)
        strength = min(
            self.config.orderbook_max_strength,
            (imbalance_factor - 1) * 10,
        )
        strength = max(5, strength)

        description = reasons[0] if reasons else f"Order book imbalance (ratio: {ratio:.1f})"

        return Signal(
            signal_type="orderbook_imbalance",
            description=description,
            strength=strength,
            data={
                "bid_ask_ratio": ratio,
                "bid_depth": analysis.get("bid_depth", 0),
                "ask_depth": analysis.get("ask_depth", 0),
                "spread": analysis.get("spread", 0),
                "all_reasons": reasons,
            },
        )

    def _detect_no_news_move(self, market: Market) -> Optional[Signal]:
        """
        The strongest signal: market moves significantly with ZERO news coverage.
        This is the "insider information" detector.
        """
        if not self.news_monitor:
            return None

        snapshots = self.db.get_recent_snapshots(
            market.platform_str,
            market.market_id,
            minutes=60,
        )

        if len(snapshots) < 2:
            return None

        oldest = snapshots[0]
        price_change = abs(market.probability - oldest["probability"])

        if price_change < self.config.no_news_min_price_change:
            return None

        news_check = self.news_monitor.check_news_coverage(
            market_name=market.name,
            market_description=market.description,
            lookback_hours=self.config.no_news_lookback_hours,
        )

        if news_check["has_news"]:
            return None

        strength = self.config.no_news_strength
        if price_change > self.config.no_news_min_price_change * 2:
            strength *= 1.3

        strength = min(30, strength)

        return Signal(
            signal_type="no_news_move",
            description=f"Moved {price_change:.1f}pp with zero news coverage",
            strength=strength,
            data={
                "price_change": price_change,
                "news_articles_found": 0,
                "search_terms": news_check.get("search_terms", []),
                "lookback_hours": self.config.no_news_lookback_hours,
            },
        )

    def _detect_whale_activity(self, market: Market) -> Optional[Signal]:
        """
        Detect large/smart money entering positions.
        """
        if not self.whale_tracker:
            return None

        activity = self.whale_tracker.get_recent_whale_activity(
            market_id=market.market_id,
            minutes=60,
        )

        if not activity or not activity.get("has_whale_activity"):
            return None

        trade_count = activity["trade_count"]
        total_volume = activity["total_volume"]
        smart_money = activity.get("smart_money_trades", 0)

        if total_volume < self.config.whale_min_trade_usd:
            return None

        strength = self.config.whale_signal_strength
        if smart_money > 0:
            strength *= 1.5
            strength = min(35, strength)

        vol_str = f"${total_volume:,.0f}"
        smart_str = f", {smart_money} smart money" if smart_money > 0 else ""

        return Signal(
            signal_type="whale_activity",
            description=f"Whale activity: {trade_count} trades ({vol_str}{smart_str})",
            strength=strength,
            data={
                "trade_count": trade_count,
                "total_volume": total_volume,
                "smart_money_trades": smart_money,
                "top_wallets": activity.get("top_wallets", []),
            },
        )

    # ==================== Scoring ====================

    def _apply_late_stage_boost(self, base_score: float, market: Market) -> float:
        """Boost signal score for markets approaching resolution."""
        days = market.days_until_resolution
        if days is None:
            return base_score
        if days > self.config.late_stage_days_threshold:
            return base_score

        days_factor = 1 - (days / self.config.late_stage_days_threshold)
        boost = 1 + (self.config.late_stage_multiplier - 1) * days_factor
        boosted = base_score * boost
        return min(100, boosted)

    def _apply_correlation_bonus(
        self,
        base_score: float,
        signals: List[Signal],
    ) -> float:
        """
        Multi-signal correlation scoring: coincidence bonus.
        When 3+ DIFFERENT signal types fire simultaneously, that's
        exponentially more meaningful than any single signal.
        """
        unique_types = set(s.signal_type for s in signals)
        n = len(unique_types)

        if n >= 5:
            bonus = self.config.correlation_5_signal_bonus
        elif n >= 4:
            bonus = self.config.correlation_4_signal_bonus
        elif n >= 3:
            bonus = self.config.correlation_3_signal_bonus
        else:
            return base_score

        boosted = base_score * bonus

        if bonus > 1.0:
            logger.info(
                f"Correlation bonus: {n} signal types → {bonus:.1f}x "
                f"({base_score:.1f} → {boosted:.1f})"
            )

        return boosted

    def calculate_signal_score(
        self,
        signals: List[Signal],
        market: Market,
    ) -> float:
        """
        Calculate composite signal score (0-100) from individual signals.

        Scoring pipeline:
        1. Sum individual signal strengths
        2. Cap base score at 85
        3. Apply multi-signal correlation bonus (NEW)
        4. Apply late-stage boost
        5. Final cap at 100
        """
        if not signals:
            return 0.0

        base_score = sum(s.strength for s in signals)
        base_score = min(85, base_score)

        correlated_score = self._apply_correlation_bonus(base_score, signals)
        boosted_score = self._apply_late_stage_boost(correlated_score, market)
        final_score = min(100, boosted_score)

        return final_score

    def create_alert(
        self,
        market: Market,
        signals: List[Signal],
        score: float,
        paired_market: Optional[Market] = None,
    ) -> Alert:
        """Create an Alert object from detected signals."""
        snapshots = self.db.get_recent_snapshots(
            market.platform_str,
            market.market_id,
            minutes=60,
        )

        old_probability = None
        time_delta = None

        if snapshots:
            oldest = snapshots[0]
            old_probability = oldest["probability"]
            try:
                oldest_time = datetime.fromisoformat(oldest["timestamp"])
                oldest_time = ensure_aware(oldest_time)
                time_delta = (utcnow() - oldest_time).total_seconds() / 60
            except (ValueError, KeyError):
                pass

        alert = Alert(
            market=market,
            signal_score=score,
            signals=signals,
            old_probability=old_probability,
            new_probability=market.probability,
            time_delta_minutes=time_delta,
        )

        if paired_market:
            alert.other_platform_probability = paired_market.probability
            alert.other_platform_name = paired_market.platform_str.title()

        return alert

    def should_alert(self, score: float) -> bool:
        """Check if score exceeds alert threshold."""
        return score >= self.config.alert_threshold

    def auto_tune_thresholds(
        self,
        performance: Dict[str, float],
        target_precision: float,
        min_recall: float,
        step_fraction: float = 0.05,
        max_step_fraction: float = 0.15,
    ) -> Dict[str, float]:
        """
        Adapt thresholds based on recent labeled performance.
        Updates are bounded and conservative to avoid oscillation.
        """
        precision = float(performance.get("precision", 0.0))
        recall = float(performance.get("recall", 0.0))

        if precision <= 0 and recall <= 0:
            return {}

        direction = 0
        gap = 0.0
        if precision < target_precision:
            direction = 1  # tighten
            gap = target_precision - precision
        elif recall < min_recall:
            direction = -1  # loosen
            gap = min_recall - recall
        else:
            return {}

        step = min(max_step_fraction, max(step_fraction, gap * 0.5))

        def clamp(v: float, lo: float, hi: float) -> float:
            return max(lo, min(hi, v))

        updates: Dict[str, float] = {}

        self.config.alert_threshold = clamp(
            self.config.alert_threshold * (1 + direction * step), 15.0, 90.0
        )
        updates["alert_threshold"] = self.config.alert_threshold

        self.config.price_velocity_min_change = clamp(
            self.config.price_velocity_min_change * (1 + direction * step), 2.0, 20.0
        )
        updates["price_velocity_min_change"] = self.config.price_velocity_min_change

        self.config.volume_shock_multiplier = clamp(
            self.config.volume_shock_multiplier * (1 + direction * step), 1.2, 8.0
        )
        updates["volume_shock_multiplier"] = self.config.volume_shock_multiplier

        self.config.cross_market_divergence_threshold = clamp(
            self.config.cross_market_divergence_threshold * (1 + direction * step), 3.0, 25.0
        )
        updates["cross_market_divergence_threshold"] = self.config.cross_market_divergence_threshold

        logger.info(
            "Auto-tune applied: precision=%.3f recall=%.3f direction=%s step=%.3f",
            precision,
            recall,
            "tighten" if direction > 0 else "loosen",
            step,
        )
        return updates


class MarketMatcher:
    """
    Matches similar markets across platforms for divergence detection.
    Uses simple text similarity - not perfect but good enough for v0.
    """

    def __init__(self):
        self._pairs: Dict[str, str] = {}

    def find_pairs(
        self,
        polymarket_markets: List[Market],
        kalshi_markets: List[Market],
    ) -> List[MarketPair]:
        """Find matching markets between platforms."""
        pairs = []
        matched_kalshi_ids: Set[str] = set()

        kalshi_by_name: Dict[str, Market] = {}
        for km in kalshi_markets:
            normalized = self._normalize_name(km.name)
            kalshi_by_name[normalized] = km

        for pm in polymarket_markets:
            pm_normalized = self._normalize_name(pm.name)

            if pm_normalized in kalshi_by_name:
                km = kalshi_by_name[pm_normalized]
                if km.market_id not in matched_kalshi_ids:
                    pairs.append(MarketPair(polymarket=pm, kalshi=km))
                    matched_kalshi_ids.add(km.market_id)
                continue

            for kalshi_name, km in kalshi_by_name.items():
                if km.market_id in matched_kalshi_ids:
                    continue
                if self._names_match(pm_normalized, kalshi_name):
                    pairs.append(MarketPair(polymarket=pm, kalshi=km))
                    matched_kalshi_ids.add(km.market_id)
                    break

        logger.info(f"Found {len(pairs)} cross-platform market pairs")
        return pairs

    def _normalize_name(self, name: str) -> str:
        normalized = name.lower()
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized)
        return normalized.strip()

    def _names_match(self, name1: str, name2: str) -> bool:
        words1 = set(name1.split())
        words2 = set(name2.split())
        if not words1 or not words2:
            return False
        overlap = len(words1 & words2)
        min_words = min(len(words1), len(words2))
        return overlap / min_words > 0.6
