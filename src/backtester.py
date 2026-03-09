"""
Historical Backtest Harness — replay past forecasts with different weights.

Loads stored forecast_asset_calls (with their driver_details) and
forecast_asset_outcomes, re-weights driver contributions, recomputes
direction & confidence, and compares against original performance.

This does NOT re-run signal extraction (would need historical market state).
It validates weight changes, which is the primary optimization lever.

Usage:
    python3 src/backtester.py --weights momentum=0.40,prediction_market=0.20 --lookback 30
    python3 src/backtester.py --disable whale,news_sentiment --lookback 14
    python3 src/backtester.py --confidence-threshold 40 --lookback 30
    python3 src/backtester.py --json
"""

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# Add parent dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import Database
from forecast_engine import DEFAULT_WEIGHTS


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class PredictionResult:
    """One recomputed prediction."""
    ticker: str
    horizon: str
    original_direction: str
    original_confidence: int
    simulated_direction: str
    simulated_confidence: int
    actual_return_pct: float
    original_dir_correct: bool
    simulated_dir_correct: bool
    original_brier: float
    simulated_brier: float
    original_log_loss: float
    simulated_log_loss: float


@dataclass
class BacktestReport:
    """Summary of backtest results."""
    total_predictions: int
    lookback_days: int
    weights_used: Dict[str, float]
    disabled_families: List[str]
    confidence_threshold: int

    # Original metrics
    orig_direction_accuracy: float
    orig_brier: float
    orig_log_loss: float

    # Simulated metrics
    sim_direction_accuracy: float
    sim_brier: float
    sim_log_loss: float

    # Deltas
    direction_accuracy_delta: float
    brier_delta: float
    log_loss_delta: float

    # Per-asset breakdown
    per_asset: Dict[str, Dict[str, Any]]

    # Detail
    direction_changes: int
    improvements: int
    degradations: int
    predictions: List[PredictionResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def recompute_forecast(
    drivers_json: str,
    override_weights: Dict[str, float],
    disabled_families: List[str],
) -> Tuple[str, int, float]:
    """
    Re-weight stored drivers and recompute direction + confidence.

    Args:
        drivers_json: JSON array of driver dicts from forecast_asset_calls
        override_weights: weight overrides per family
        disabled_families: families to zero out

    Returns:
        (direction, confidence, net_pressure)
    """
    try:
        drivers = json.loads(drivers_json or "[]")
    except Exception:
        return ("UP", 20, 0.0)

    if not drivers:
        return ("UP", 20, 0.0)

    # Recompute contributions with new weights
    reweighted = []
    for d in drivers:
        family = d.get("family", "")
        original_value = d.get("value", 0.0)

        if family in disabled_families:
            continue

        new_weight = override_weights.get(family, d.get("weight", 0.0))
        new_contribution = original_value * new_weight
        reweighted.append({
            "value": original_value,
            "weight": new_weight,
            "contribution": new_contribution,
            "family": family,
        })

    if not reweighted:
        return ("UP", 20, 0.0)

    net_pressure = sum(d["contribution"] for d in reweighted)
    direction = "UP" if net_pressure >= 0 else "DOWN"

    # Recompute confidence (same algorithm as ForecastEngine._compute_confidence)
    dir_sign = 1 if net_pressure >= 0 else -1
    agreeing = sum(
        1 for d in reweighted
        if (d["value"] > 0) == (dir_sign > 0) and abs(d["value"]) > 0.01
    )
    total_active = len([d for d in reweighted if abs(d["value"]) > 0.01])

    if total_active == 0:
        confidence = 20
    else:
        agreement_ratio = agreeing / total_active
        base = 20 + int(agreement_ratio * 50)
        avg_strength = sum(abs(d["value"]) for d in reweighted) / len(reweighted)
        strength_boost = int(avg_strength * 25)
        confidence = min(95, base + strength_boost)

    return (direction, confidence, net_pressure)


def score_prediction(
    direction: str,
    confidence: int,
    actual_return: float,
) -> Tuple[bool, float, float]:
    """
    Score a single prediction against actual outcome.

    Returns:
        (direction_correct, brier_score, log_loss)
    """
    if direction == "UP":
        dir_correct = actual_return > 0.15
    else:
        dir_correct = actual_return < -0.15

    p_predicted = confidence / 100.0
    actual_binary = 1.0 if dir_correct else 0.0
    brier = (p_predicted - actual_binary) ** 2

    p_for_actual = p_predicted if dir_correct else (1 - p_predicted)
    p_for_actual = max(0.001, min(0.999, p_for_actual))
    log_loss = -math.log(p_for_actual)

    return (dir_correct, brier, log_loss)


def run_backtest(
    db: Database,
    override_weights: Optional[Dict[str, float]] = None,
    disabled_families: Optional[List[str]] = None,
    lookback_days: int = 30,
    confidence_threshold: int = 0,
) -> BacktestReport:
    """
    Replay historical forecast calls with different weights.

    Args:
        db: Database instance
        override_weights: Weight overrides (merged with defaults)
        disabled_families: Signal families to disable
        lookback_days: How many days of history to replay
        confidence_threshold: Only include predictions >= this confidence

    Returns:
        BacktestReport with original vs simulated comparison
    """
    disabled = disabled_families or []

    # Build effective weight set
    weights = dict(DEFAULT_WEIGHTS)
    if override_weights:
        for k, v in override_weights.items():
            if k in weights:
                weights[k] = v

    # Zero out disabled families
    for fam in disabled:
        if fam in weights:
            weights[fam] = 0.0

    # Normalize to sum = 1
    total_w = sum(weights.values())
    if total_w > 0:
        weights = {k: v / total_w for k, v in weights.items()}

    # Load outcomes (joined with call data)
    outcomes = db.get_recent_forecast_outcomes(limit=lookback_days * 24 * 2)

    # Filter by lookback
    cutoff = (_utcnow() - timedelta(days=lookback_days)).isoformat()
    outcomes = [o for o in outcomes if (o.get("call_generated_at") or "") >= cutoff]

    predictions: List[PredictionResult] = []
    per_asset_stats: Dict[str, Dict[str, list]] = {}

    for o in outcomes:
        ticker = o.get("ticker", "?")
        horizon = o.get("horizon", "?")
        drivers_json = o.get("drivers_json", "[]")
        actual_return = o.get("actual_return_pct", 0.0)
        original_dir = o.get("direction", "UP")
        original_conf = o.get("confidence", 50)

        # Apply confidence threshold
        if original_conf < confidence_threshold:
            continue

        # Original scores
        orig_dir_correct, orig_brier, orig_log_loss = score_prediction(
            original_dir, original_conf, actual_return
        )

        # Simulated scores
        sim_dir, sim_conf, _sim_pressure = recompute_forecast(
            drivers_json, weights, disabled
        )
        sim_dir_correct, sim_brier, sim_log_loss = score_prediction(
            sim_dir, sim_conf, actual_return
        )

        result = PredictionResult(
            ticker=ticker,
            horizon=horizon,
            original_direction=original_dir,
            original_confidence=original_conf,
            simulated_direction=sim_dir,
            simulated_confidence=sim_conf,
            actual_return_pct=actual_return,
            original_dir_correct=orig_dir_correct,
            simulated_dir_correct=sim_dir_correct,
            original_brier=orig_brier,
            simulated_brier=sim_brier,
            original_log_loss=orig_log_loss,
            simulated_log_loss=sim_log_loss,
        )
        predictions.append(result)

        # Per-asset tracking
        if ticker not in per_asset_stats:
            per_asset_stats[ticker] = {
                "orig_correct": [], "sim_correct": [],
                "orig_brier": [], "sim_brier": [],
                "orig_log_loss": [], "sim_log_loss": [],
            }
        per_asset_stats[ticker]["orig_correct"].append(orig_dir_correct)
        per_asset_stats[ticker]["sim_correct"].append(sim_dir_correct)
        per_asset_stats[ticker]["orig_brier"].append(orig_brier)
        per_asset_stats[ticker]["sim_brier"].append(sim_brier)
        per_asset_stats[ticker]["orig_log_loss"].append(orig_log_loss)
        per_asset_stats[ticker]["sim_log_loss"].append(sim_log_loss)

    n = len(predictions)
    if n == 0:
        return BacktestReport(
            total_predictions=0,
            lookback_days=lookback_days,
            weights_used=weights,
            disabled_families=disabled,
            confidence_threshold=confidence_threshold,
            orig_direction_accuracy=0, orig_brier=0, orig_log_loss=0,
            sim_direction_accuracy=0, sim_brier=0, sim_log_loss=0,
            direction_accuracy_delta=0, brier_delta=0, log_loss_delta=0,
            per_asset={}, direction_changes=0, improvements=0, degradations=0,
        )

    # Aggregates
    orig_dir_acc = sum(1 for p in predictions if p.original_dir_correct) / n
    sim_dir_acc = sum(1 for p in predictions if p.simulated_dir_correct) / n
    orig_brier_avg = sum(p.original_brier for p in predictions) / n
    sim_brier_avg = sum(p.simulated_brier for p in predictions) / n
    orig_ll_avg = sum(p.original_log_loss for p in predictions) / n
    sim_ll_avg = sum(p.simulated_log_loss for p in predictions) / n

    # Direction changes
    direction_changes = sum(
        1 for p in predictions
        if p.original_direction != p.simulated_direction
    )
    improvements = sum(
        1 for p in predictions
        if not p.original_dir_correct and p.simulated_dir_correct
    )
    degradations = sum(
        1 for p in predictions
        if p.original_dir_correct and not p.simulated_dir_correct
    )

    # Per-asset summary
    per_asset: Dict[str, Dict[str, Any]] = {}
    for ticker, stats in per_asset_stats.items():
        n_asset = len(stats["orig_correct"])
        per_asset[ticker] = {
            "count": n_asset,
            "orig_dir_acc": round(
                sum(stats["orig_correct"]) / n_asset, 4
            ) if n_asset else 0,
            "sim_dir_acc": round(
                sum(stats["sim_correct"]) / n_asset, 4
            ) if n_asset else 0,
            "orig_brier": round(
                sum(stats["orig_brier"]) / n_asset, 4
            ) if n_asset else 0,
            "sim_brier": round(
                sum(stats["sim_brier"]) / n_asset, 4
            ) if n_asset else 0,
            "delta_dir_acc": round(
                (sum(stats["sim_correct"]) - sum(stats["orig_correct"])) / n_asset, 4
            ) if n_asset else 0,
        }

    return BacktestReport(
        total_predictions=n,
        lookback_days=lookback_days,
        weights_used={k: round(v, 4) for k, v in weights.items()},
        disabled_families=disabled,
        confidence_threshold=confidence_threshold,
        orig_direction_accuracy=round(orig_dir_acc, 4),
        orig_brier=round(orig_brier_avg, 4),
        orig_log_loss=round(orig_ll_avg, 4),
        sim_direction_accuracy=round(sim_dir_acc, 4),
        sim_brier=round(sim_brier_avg, 4),
        sim_log_loss=round(sim_ll_avg, 4),
        direction_accuracy_delta=round(sim_dir_acc - orig_dir_acc, 4),
        brier_delta=round(sim_brier_avg - orig_brier_avg, 4),
        log_loss_delta=round(sim_ll_avg - orig_ll_avg, 4),
        per_asset=per_asset,
        direction_changes=direction_changes,
        improvements=improvements,
        degradations=degradations,
        predictions=predictions,
    )


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def format_report(report: BacktestReport) -> str:
    """Format a BacktestReport as a human-readable string."""
    lines = []
    lines.append("=" * 72)
    lines.append("  HISTORICAL BACKTEST REPORT")
    lines.append("=" * 72)
    lines.append(f"  Predictions replayed:  {report.total_predictions}")
    lines.append(f"  Lookback:              {report.lookback_days} days")
    lines.append(f"  Confidence threshold:  {report.confidence_threshold}%")
    if report.disabled_families:
        lines.append(
            f"  Disabled families:     {', '.join(report.disabled_families)}"
        )
    lines.append("")

    # Weights
    lines.append("  WEIGHTS USED:")
    for fam, w in sorted(report.weights_used.items(), key=lambda x: -x[1]):
        default = DEFAULT_WEIGHTS.get(fam, 0)
        marker = " *" if abs(w - default) > 0.005 else ""
        lines.append(f"    {fam:25s} {w:.4f}  (default {default:.2f}){marker}")
    lines.append("")

    # Comparison table
    lines.append("  PERFORMANCE COMPARISON:")
    lines.append(
        f"  {'Metric':<25s} {'Original':>10s} {'Simulated':>10s} {'Delta':>10s}"
    )
    lines.append("  " + "-" * 57)

    def _fmt_delta(d: float, lower_better: bool = False) -> str:
        if abs(d) < 0.0001:
            return "     --"
        arrow = "v" if d < 0 else "^"
        check = "+" if (d < 0) == lower_better else "-"
        return f"{d:+.4f} {arrow}{check}"

    lines.append(
        f"  {'Direction accuracy':<25s} {report.orig_direction_accuracy:>10.4f} "
        f"{report.sim_direction_accuracy:>10.4f} "
        f"{_fmt_delta(report.direction_accuracy_delta):>10s}"
    )
    lines.append(
        f"  {'Brier score':<25s} {report.orig_brier:>10.4f} "
        f"{report.sim_brier:>10.4f} "
        f"{_fmt_delta(report.brier_delta, lower_better=True):>10s}"
    )
    lines.append(
        f"  {'Log loss':<25s} {report.orig_log_loss:>10.4f} "
        f"{report.sim_log_loss:>10.4f} "
        f"{_fmt_delta(report.log_loss_delta, lower_better=True):>10s}"
    )
    lines.append("")
    lines.append(
        f"  Direction changes: {report.direction_changes}  "
        f"(improvements: {report.improvements}, degradations: {report.degradations})"
    )
    lines.append("")

    # Per-asset breakdown
    if report.per_asset:
        lines.append("  PER-ASSET BREAKDOWN:")
        lines.append(
            f"  {'Ticker':<8s} {'N':>5s} {'Orig Acc':>10s} "
            f"{'Sim Acc':>10s} {'d Acc':>8s} "
            f"{'Orig Brier':>12s} {'Sim Brier':>12s}"
        )
        lines.append("  " + "-" * 67)
        for ticker in sorted(report.per_asset.keys()):
            s = report.per_asset[ticker]
            delta_str = (
                f"{s['delta_dir_acc']:+.4f}"
                if s["delta_dir_acc"] != 0
                else "    --"
            )
            lines.append(
                f"  {ticker:<8s} {s['count']:>5d} "
                f"{s['orig_dir_acc']:>10.4f} {s['sim_dir_acc']:>10.4f} "
                f"{delta_str:>8s} "
                f"{s['orig_brier']:>12.4f} {s['sim_brier']:>12.4f}"
            )
        lines.append("")

    lines.append("=" * 72)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Replay historical forecasts with different signal weights."
    )
    parser.add_argument(
        "--weights", type=str, default="",
        help=(
            "Weight overrides as key=value pairs: "
            "momentum=0.40,prediction_market=0.20"
        ),
    )
    parser.add_argument(
        "--disable", type=str, default="",
        help="Comma-separated signal families to disable: whale,news_sentiment",
    )
    parser.add_argument(
        "--lookback", type=int, default=30,
        help="Days of history to replay (default: 30)",
    )
    parser.add_argument(
        "--confidence-threshold", type=int, default=0,
        help="Only include predictions >= this confidence (default: 0)",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output machine-readable JSON instead of formatted report",
    )
    parser.add_argument(
        "--db", type=str, default="market_sentinel.db",
        help="Path to database file (default: market_sentinel.db)",
    )

    args = parser.parse_args()

    # Parse weight overrides
    override_weights: Dict[str, float] = {}
    if args.weights:
        for pair in args.weights.split(","):
            pair = pair.strip()
            if "=" in pair:
                k, v = pair.split("=", 1)
                k = k.strip()
                try:
                    override_weights[k] = float(v.strip())
                except ValueError:
                    print(
                        f"Warning: ignoring invalid weight '{pair}'",
                        file=sys.stderr,
                    )

    # Parse disabled families
    disabled = (
        [f.strip() for f in args.disable.split(",") if f.strip()]
        if args.disable
        else []
    )

    # Connect to DB
    db = Database(args.db)

    # Run backtest
    report = run_backtest(
        db=db,
        override_weights=override_weights if override_weights else None,
        disabled_families=disabled if disabled else None,
        lookback_days=args.lookback,
        confidence_threshold=args.confidence_threshold,
    )

    if args.json:
        output = {
            "total_predictions": report.total_predictions,
            "lookback_days": report.lookback_days,
            "weights_used": report.weights_used,
            "disabled_families": report.disabled_families,
            "confidence_threshold": report.confidence_threshold,
            "original": {
                "direction_accuracy": report.orig_direction_accuracy,
                "brier": report.orig_brier,
                "log_loss": report.orig_log_loss,
            },
            "simulated": {
                "direction_accuracy": report.sim_direction_accuracy,
                "brier": report.sim_brier,
                "log_loss": report.sim_log_loss,
            },
            "deltas": {
                "direction_accuracy": report.direction_accuracy_delta,
                "brier": report.brier_delta,
                "log_loss": report.log_loss_delta,
            },
            "direction_changes": report.direction_changes,
            "improvements": report.improvements,
            "degradations": report.degradations,
            "per_asset": report.per_asset,
        }
        print(json.dumps(output, indent=2))
    else:
        if report.total_predictions == 0:
            print(
                "\nNo forecast outcomes found in the database "
                "for the specified lookback period."
            )
            print(
                f"Looked back {args.lookback} days. "
                "Try increasing --lookback or running the system longer.\n"
            )
        else:
            print(format_report(report))


if __name__ == "__main__":
    main()
