"""
Hardened Forecast Evaluator — grades predictions, learns weights, tracks baselines.

Responsibilities:
  1. grade_pending()    — score ungraded forecast_asset_calls vs actual prices
  2. update_weights()   — slow-learning weight adjustment with safeguards
  3. get_evaluation()   — full payload for /api/forecast/evaluation
"""

import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# Reuse from forecast_engine to avoid circular imports
DEFAULT_WEIGHTS: Dict[str, float] = {
    "prediction_market": 0.30,
    "whale":             0.20,
    "momentum":          0.20,
    "cross_asset":       0.15,
    "news_sentiment":    0.15,
}

MAGNITUDE_TIERS = {1: (0, 0.5), 2: (0.5, 1.5), 3: (1.5, 3.0), 4: (3.0, 100.0)}


def _magnitude_tier(pct_change: float) -> int:
    a = abs(pct_change)
    if a < 0.5:  return 1
    if a < 1.5:  return 2
    if a < 3.0:  return 3
    return 4


MAGNITUDE_LABEL_MAP = {"SMALL": 1, "MODERATE": 2, "LARGE": 3, "MAJOR": 4}

# Learning parameters
LEARNING_RATE = 0.02
SHRINKAGE_FACTOR = 0.90     # 90% learned + 10% default
WEIGHT_FLOOR = 0.05
WEIGHT_CEILING = 0.60
MIN_SAMPLES_FOR_LEARNING = 20
ROLLING_WINDOW = 50
COOLDOWN_THRESHOLD = 3       # consecutive updates worse than baseline


def _pav_isotonic(points: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    """
    Pool Adjacent Violators (PAV) algorithm for isotonic regression.

    Takes a list of (predicted_confidence, actual_outcome) sorted by predicted
    and returns a monotonically non-decreasing calibration curve.

    This is the standard algorithm for making probability predictions honest:
    if you say 70%, roughly 70% of those predictions should be correct.

    Args:
        points: sorted list of (predicted_prob_0to1, actual_binary_0or1)

    Returns:
        list of (predicted_prob, calibrated_prob) — monotone non-decreasing
    """
    if len(points) < 2:
        return points

    # Initialize blocks: each point is its own block
    # Block = [sum_of_actuals, count, min_pred, max_pred]
    blocks = []
    for pred, actual in points:
        blocks.append([float(actual), 1, pred, pred])

    # Merge adjacent blocks that violate isotonicity
    changed = True
    while changed:
        changed = False
        merged = []
        i = 0
        while i < len(blocks):
            if i + 1 < len(blocks):
                mean_i = blocks[i][0] / blocks[i][1]
                mean_j = blocks[i + 1][0] / blocks[i + 1][1]
                if mean_i > mean_j:
                    # Merge blocks i and i+1
                    merged_block = [
                        blocks[i][0] + blocks[i + 1][0],   # sum actuals
                        blocks[i][1] + blocks[i + 1][1],   # count
                        blocks[i][2],                        # min_pred (from left)
                        blocks[i + 1][3],                    # max_pred (from right)
                    ]
                    merged.append(merged_block)
                    i += 2
                    changed = True
                    continue
            merged.append(blocks[i])
            i += 1
        blocks = merged

    # Build output curve: one point per block at the block's midpoint prediction
    curve = []
    for s, n, pred_min, pred_max in blocks:
        # Laplace smoothing: (correct + 1) / (total + 2) — prevents 0% or 100%
        calibrated_smooth = (s + 1) / (n + 2)
        mid_pred = (pred_min + pred_max) / 2.0
        curve.append((mid_pred, calibrated_smooth))

    # Laplace smoothing can break monotonicity for blocks of different sizes
    # with the same raw average. Enforce monotonicity with a forward pass.
    for i in range(1, len(curve)):
        if curve[i][1] < curve[i - 1][1]:
            curve[i] = (curve[i][0], curve[i - 1][1])

    return curve


class ForecastEvaluator:
    """
    Grades forecast_asset_calls against actual market prices and
    updates signal weights using a slow, bounded learning loop.
    """

    HAIKU_MODEL = "claude-haiku-4-5"

    def __init__(self, api_key: str = ""):
        key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._client = None
        if key:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=key)
            except Exception:
                pass

    # ── Grading ────────────────────────────────────────────────────────

    def grade_pending(self, db, market_data) -> int:
        """
        Find ungraded forecast_asset_calls, score against actual prices,
        write to forecast_asset_outcomes + backward-compat outlook_grades.
        Returns number of new outcomes written.
        """
        new_outcomes = 0

        for horizon in ("24h", "48h"):
            calls = db.get_ungraded_forecast_calls(horizon)
            if not calls:
                continue

            # Group by session for backward-compat outlook_grades
            session_grades: Dict[str, list] = {}

            for call in calls:
                try:
                    outcome = self._grade_call(call, market_data)
                    if outcome is None:
                        continue

                    db.save_forecast_outcome(outcome)
                    new_outcomes += 1

                    # Collect for session-level outlook_grades
                    sid = call["session_id"]
                    if sid not in session_grades:
                        session_grades[sid] = []
                    session_grades[sid].append({
                        "call": call,
                        "outcome": outcome,
                    })

                except Exception as e:
                    logger.warning(
                        f"ForecastEvaluator: grading error for call {call.get('id')}: {e}"
                    )

            # Write backward-compatible outlook_grades per session
            for sid, items in session_grades.items():
                try:
                    self._write_outlook_grade(db, sid, horizon, items)
                except Exception as e:
                    logger.debug(f"ForecastEvaluator: outlook_grade write failed: {e}")

        if new_outcomes > 0:
            logger.info(f"ForecastEvaluator: graded {new_outcomes} new outcomes")
            # Generate reflection on latest grades
            self._refresh_reflection(db)

        return new_outcomes

    def _grade_call(self, call: dict, market_data) -> Optional[Dict]:
        """Grade a single forecast_asset_call against actual prices."""
        ticker = call["ticker"]
        horizon = call["horizon"]
        generated_at = datetime.fromisoformat(call["generated_at"])
        horizon_hours = 24 if horizon == "24h" else 48
        end_dt = generated_at + timedelta(hours=horizon_hours)

        # Too early to grade?
        if end_dt > _utcnow():
            return None

        # Get prices
        price_start = market_data.get_price_at(ticker, generated_at)
        price_end = market_data.get_price_at(ticker, end_dt)

        if price_start is None or price_end is None or price_start == 0:
            return None
        if abs(price_end - price_start) < 1e-8:
            return None  # no real data yet

        actual_return = (price_end - price_start) / price_start * 100

        # Direction correctness
        predicted_dir = call["direction"]
        # Simple direction check (threshold at 0.15% to avoid noise)
        if predicted_dir == "UP":
            dir_correct = 1 if actual_return > 0.15 else 0
        else:
            dir_correct = 1 if actual_return < -0.15 else 0

        # Magnitude correctness
        actual_mag = _magnitude_tier(actual_return)
        predicted_mag_str = call.get("magnitude", "SMALL")
        predicted_mag = MAGNITUDE_LABEL_MAP.get(predicted_mag_str, 1)
        mag_correct = 1 if actual_mag == predicted_mag else 0

        # Brier score: (predicted_probability - actual_outcome)^2
        confidence = call.get("confidence", 50)
        p_predicted = confidence / 100.0
        actual_binary = float(dir_correct)
        brier = (p_predicted - actual_binary) ** 2

        # Log loss: -log(p) where p is predicted prob for actual outcome
        p_for_actual = p_predicted if dir_correct else (1 - p_predicted)
        p_for_actual = max(0.001, min(0.999, p_for_actual))  # clamp to avoid log(0)
        log_loss = -math.log(p_for_actual)

        return {
            "call_id": call["id"],
            "graded_at": _utcnow().isoformat(),
            "price_start": round(price_start, 4),
            "price_end": round(price_end, 4),
            "actual_return_pct": round(actual_return, 4),
            "direction_correct": dir_correct,
            "magnitude_correct": mag_correct,
            "brier_score": round(brier, 4),
            "log_loss": round(log_loss, 4),
        }

    def _write_outlook_grade(self, db, session_id: str, horizon: str, items: list):
        """Write backward-compatible outlook_grades from forecast outcomes."""
        grades_dict = {}
        correct = 0
        total = 0

        for item in items:
            call = item["call"]
            outcome = item["outcome"]
            ticker = call["ticker"]

            grades_dict[ticker] = {
                "predicted_direction": call["direction"],
                "actual_direction": "UP" if outcome["actual_return_pct"] > 0 else "DOWN",
                "predicted_magnitude": MAGNITUDE_LABEL_MAP.get(call.get("magnitude", "SMALL"), 1),
                "predicted_magnitude_label": call.get("magnitude", "SMALL"),
                "actual_magnitude": _magnitude_tier(outcome["actual_return_pct"]),
                "actual_change_pct": outcome["actual_return_pct"],
                "direction_correct": bool(outcome["direction_correct"]),
                "magnitude_score": 1.0 if outcome["magnitude_correct"] else 0.6,
                "composite_score": round(
                    outcome["direction_correct"] * 0.7 +
                    (1.0 if outcome["magnitude_correct"] else 0.6) * 0.3, 3
                ),
                "price_start": outcome["price_start"],
                "price_end": outcome["price_end"],
                "confidence": call.get("confidence", 50),
                "brier_score": outcome.get("brier_score"),
                "log_loss": outcome.get("log_loss"),
            }

            if outcome["direction_correct"]:
                correct += 1
            total += 1

        if total == 0:
            return

        dir_acc = round(correct / total, 3)
        overall = round(sum(g["composite_score"] for g in grades_dict.values()) / len(grades_dict), 3)

        db.save_outlook_grade(
            session_id=session_id,
            horizon=horizon,
            graded_at=_utcnow().isoformat(),
            overall_score=overall,
            direction_accuracy=dir_acc,
            grades_json=json.dumps(grades_dict),
            reflection="",
        )

    # ── Weight Learning ────────────────────────────────────────────────

    def update_weights(self, db) -> Dict[str, Any]:
        """
        Update signal family weights based on recent grading outcomes.
        Returns a report dict with new weights, driver quality, and baseline comparison.
        """
        outcomes = db.get_recent_forecast_outcomes(limit=ROLLING_WINDOW * 12)

        if len(outcomes) < MIN_SAMPLES_FOR_LEARNING:
            logger.info(
                f"ForecastEvaluator: only {len(outcomes)} outcomes, "
                f"need {MIN_SAMPLES_FOR_LEARNING} — skipping weight update"
            )
            return {"status": "insufficient_data", "count": len(outcomes)}

        # Parse driver families from each outcome's call
        family_stats: Dict[str, Dict[str, float]] = {}
        for fam in DEFAULT_WEIGHTS:
            family_stats[fam] = {"correct_contrib": 0, "incorrect_contrib": 0,
                                  "correct_count": 0, "incorrect_count": 0}

        model_brier_sum = 0.0
        neutral_brier_sum = 0.0
        momentum_brier_sum = 0.0
        total_scored = 0

        for o in outcomes:
            dir_correct = o.get("direction_correct", 0)
            brier = o.get("brier_score", 0)
            confidence = o.get("confidence", 50)

            model_brier_sum += brier
            # Neutral baseline: always predict 50% → brier = (0.5 - actual)^2
            neutral_brier_sum += (0.5 - float(dir_correct)) ** 2
            total_scored += 1

            # Parse drivers to attribute quality per family
            try:
                drivers = json.loads(o.get("drivers_json") or "[]")
            except Exception:
                drivers = []

            for d in drivers:
                fam = d.get("family", "")
                if fam not in family_stats:
                    continue
                contrib = abs(d.get("contribution", 0))
                if dir_correct:
                    family_stats[fam]["correct_contrib"] += contrib
                    family_stats[fam]["correct_count"] += 1
                else:
                    family_stats[fam]["incorrect_contrib"] += contrib
                    family_stats[fam]["incorrect_count"] += 1

        # Compute per-family quality score: correct_rate weighted by contribution
        driver_quality: Dict[str, float] = {}
        for fam, stats in family_stats.items():
            total_fam = stats["correct_count"] + stats["incorrect_count"]
            if total_fam < 5:
                driver_quality[fam] = 0.5  # neutral — insufficient data
            else:
                correct_rate = stats["correct_count"] / total_fam
                # Weight by how much this family contributed
                avg_contrib = (stats["correct_contrib"] + stats["incorrect_contrib"]) / total_fam
                driver_quality[fam] = round(correct_rate * (1 + avg_contrib), 4)

        # Load current weights
        stored = db.get_state("forecast_signal_weights", default=None)
        current_weights = dict(DEFAULT_WEIGHTS)
        confidence_modifier = 0
        consecutive_worse = 0

        if stored and isinstance(stored, dict):
            w = stored.get("weights", {})
            for k in current_weights:
                if k in w:
                    current_weights[k] = float(w[k])
            confidence_modifier = int(stored.get("confidence_modifier", 0))
            consecutive_worse = int(stored.get("consecutive_worse", 0))

        # Apply learning: move weights toward quality signal
        new_weights = {}
        for fam in DEFAULT_WEIGHTS:
            quality = driver_quality.get(fam, 0.5)
            old = current_weights.get(fam, DEFAULT_WEIGHTS[fam])

            # Slow learning: nudge toward quality
            learned = old + LEARNING_RATE * (quality - old)

            # Shrinkage toward defaults
            adjusted = SHRINKAGE_FACTOR * learned + (1 - SHRINKAGE_FACTOR) * DEFAULT_WEIGHTS[fam]

            # Bounds
            adjusted = max(WEIGHT_FLOOR, min(WEIGHT_CEILING, adjusted))
            new_weights[fam] = round(adjusted, 4)

        # Normalize to sum to 1.0
        total_w = sum(new_weights.values())
        if total_w > 0:
            new_weights = {k: round(v / total_w, 4) for k, v in new_weights.items()}

        # Baseline comparison
        model_brier = model_brier_sum / max(total_scored, 1)
        neutral_brier = neutral_brier_sum / max(total_scored, 1)

        # Momentum baseline: would need price data — approximate from outcomes
        # A simple proxy: predict direction of last 5 outcomes same ticker
        # For now just use direction accuracy
        model_dir_acc = sum(1 for o in outcomes if o.get("direction_correct")) / max(len(outcomes), 1)

        vs_baseline = {
            "model_brier": round(model_brier, 4),
            "neutral_brier": round(neutral_brier, 4),
            "model_better_than_neutral": model_brier < neutral_brier,
            "model_direction_accuracy": round(model_dir_acc, 4),
        }

        # Cooldown: if model worse than neutral for consecutive updates
        if model_brier >= neutral_brier:
            consecutive_worse += 1
        else:
            consecutive_worse = 0

        if consecutive_worse >= COOLDOWN_THRESHOLD:
            confidence_modifier = max(-20, confidence_modifier - 5)
            logger.warning(
                f"ForecastEvaluator: cooldown engaged — confidence_modifier={confidence_modifier}, "
                f"consecutive_worse={consecutive_worse}"
            )
        elif model_brier < neutral_brier * 0.9:
            # Model doing well — slowly restore confidence
            confidence_modifier = min(0, confidence_modifier + 2)

        # Persist
        state = {
            "weights": new_weights,
            "driver_quality": driver_quality,
            "confidence_modifier": confidence_modifier,
            "consecutive_worse": consecutive_worse,
            "vs_baseline": vs_baseline,
            "updated_at": _utcnow().isoformat(),
            "sample_count": total_scored,
        }
        db.set_state("forecast_signal_weights", state)

        logger.info(
            f"ForecastEvaluator: weights updated — "
            f"brier={model_brier:.4f} (neutral={neutral_brier:.4f}), "
            f"dir_acc={model_dir_acc:.3f}, conf_mod={confidence_modifier}"
        )

        return {
            "status": "updated",
            "weights": new_weights,
            "driver_quality": driver_quality,
            "vs_baseline": vs_baseline,
            "confidence_modifier": confidence_modifier,
            "sample_count": total_scored,
        }

    # ── Isotonic Calibration ──────────────────────────────────────────

    def update_calibration(self, db) -> Dict[str, Any]:
        """
        Compute isotonic calibration curve from graded outcomes and persist it.

        The curve maps raw confidence → calibrated confidence so that if the
        model says "70% confident", ~70% of those predictions are actually correct.

        Uses Pool Adjacent Violators (PAV) algorithm with Laplace smoothing.
        Requires at least MIN_SAMPLES_FOR_LEARNING outcomes.

        Returns a report dict with curve stats.
        """
        outcomes = db.get_recent_forecast_outcomes(limit=ROLLING_WINDOW * 12)

        if len(outcomes) < MIN_SAMPLES_FOR_LEARNING:
            logger.info(
                f"ForecastEvaluator: only {len(outcomes)} outcomes, "
                f"need {MIN_SAMPLES_FOR_LEARNING} for calibration — skipping"
            )
            return {"status": "insufficient_data", "count": len(outcomes)}

        # Build (predicted_prob, actual_binary) points sorted by predicted_prob
        points = []
        for o in outcomes:
            confidence = o.get("confidence")
            dir_correct = o.get("direction_correct")
            if confidence is None or dir_correct is None:
                continue
            pred_prob = float(confidence) / 100.0
            actual = float(dir_correct)
            points.append((pred_prob, actual))

        if len(points) < MIN_SAMPLES_FOR_LEARNING:
            return {"status": "insufficient_data", "count": len(points)}

        # Sort by predicted probability
        points.sort(key=lambda p: p[0])

        # Run PAV algorithm
        raw_curve = _pav_isotonic(points)

        if len(raw_curve) < 2:
            return {"status": "degenerate_curve", "count": len(points)}

        # Convert to confidence scale (0-100) for storage
        curve_100 = [(round(p * 100, 2), round(c * 100, 2)) for p, c in raw_curve]

        # Compute calibration stats
        # Expected Calibration Error (ECE): bin predictions and measure gap
        n_bins = 10
        bin_correct = [0] * n_bins
        bin_total = [0] * n_bins
        bin_conf_sum = [0.0] * n_bins
        for pred, actual in points:
            b = min(int(pred * n_bins), n_bins - 1)
            bin_total[b] += 1
            bin_correct[b] += actual
            bin_conf_sum[b] += pred

        ece = 0.0
        bins_detail = []
        for b in range(n_bins):
            if bin_total[b] > 0:
                acc = bin_correct[b] / bin_total[b]
                avg_conf = bin_conf_sum[b] / bin_total[b]
                gap = abs(acc - avg_conf)
                ece += gap * (bin_total[b] / len(points))
                bins_detail.append({
                    "bin": f"{b * 10}-{(b + 1) * 10}%",
                    "count": bin_total[b],
                    "accuracy": round(acc, 4),
                    "avg_confidence": round(avg_conf, 4),
                    "gap": round(gap, 4),
                })

        # Persist calibration curve
        state = {
            "curve": curve_100,
            "ece": round(ece, 4),
            "sample_count": len(points),
            "n_blocks": len(raw_curve),
            "updated_at": _utcnow().isoformat(),
            "bins": bins_detail,
        }
        db.set_state("forecast_calibration_curve", state)

        logger.info(
            f"ForecastEvaluator: calibration updated — "
            f"{len(points)} samples, {len(raw_curve)} isotonic blocks, ECE={ece:.4f}"
        )

        return {
            "status": "updated",
            "sample_count": len(points),
            "n_blocks": len(raw_curve),
            "ece": round(ece, 4),
            "curve": curve_100,
            "bins": bins_detail,
        }

    @staticmethod
    def get_calibration_curve(db) -> Optional[Dict]:
        """Load the current calibration curve from DB for display/API."""
        try:
            stored = db.get_state("forecast_calibration_curve", default=None)
            if stored and isinstance(stored, dict):
                return stored
        except Exception:
            pass
        return None

    # ── Evaluation Payload ─────────────────────────────────────────────

    def get_evaluation(self, db) -> Dict[str, Any]:
        """Full evaluation payload for /api/forecast/evaluation."""
        # DB stats
        eval_stats = db.get_forecast_evaluation_stats()

        # Current weights
        stored = db.get_state("forecast_signal_weights", default=None)
        current_weights = dict(DEFAULT_WEIGHTS)
        driver_quality = {}
        vs_baseline = {}
        confidence_modifier = 0

        if stored and isinstance(stored, dict):
            w = stored.get("weights", {})
            for k in current_weights:
                if k in w:
                    current_weights[k] = float(w[k])
            driver_quality = stored.get("driver_quality", {})
            vs_baseline = stored.get("vs_baseline", {})
            confidence_modifier = int(stored.get("confidence_modifier", 0))

        # Include calibration data
        calibration = self.get_calibration_curve(db)

        return {
            "stats": eval_stats,
            "current_weights": current_weights,
            "default_weights": dict(DEFAULT_WEIGHTS),
            "driver_quality": driver_quality,
            "vs_baseline": vs_baseline,
            "confidence_modifier": confidence_modifier,
            "calibration": calibration,
            "server_time": _utcnow().isoformat(),
        }

    # ── Reflection ─────────────────────────────────────────────────────

    def _refresh_reflection(self, db):
        """Generate Claude Haiku reflection on recent grades."""
        if not self._client:
            return

        recent = db.get_outlook_grades(limit=10)
        if not recent:
            return

        lines = []
        for g in recent[:6]:
            horizon = g.get("horizon", "?")
            acc = (g.get("direction_accuracy") or 0) * 100
            try:
                grades = json.loads(g.get("grades_json") or "{}")
            except Exception:
                grades = {}
            wrong = [t for t, v in grades.items() if not v.get("direction_correct")]
            right = [t for t, v in grades.items() if v.get("direction_correct")]
            date = (g.get("pred_generated_at") or "")[:10]
            regime = g.get("pred_regime") or "?"
            lines.append(
                f"- {date} {horizon} [{regime}]: {acc:.0f}% accuracy "
                f"correct={right or 'none'}, wrong={wrong or 'none'}"
            )

        if not lines:
            return

        try:
            msg = self._client.messages.create(
                model=self.HAIKU_MODEL,
                max_tokens=350,
                system=(
                    "You are a senior quant analyst reviewing an AI prediction model. "
                    "Be direct and specific."
                ),
                messages=[{"role": "user", "content": (
                    f"Recent prediction results:\n{chr(10).join(lines)}\n\n"
                    "Write exactly 3 sentences:\n"
                    "1. Which assets/conditions the model predicted best and why.\n"
                    "2. Where it consistently fails and the likely cause.\n"
                    "3. One specific, actionable change to improve accuracy."
                )}],
            )
            reflection = msg.content[0].text.strip()
            if reflection and recent:
                db.update_outlook_grade_reflection(recent[0]["id"], reflection)
        except Exception as e:
            logger.warning(f"ForecastEvaluator: reflection error: {e}")
