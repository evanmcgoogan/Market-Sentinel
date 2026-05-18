"""
score.py — SCORE stage. Emits typed Stream Updates to updates/.

Runs after COMPILE and SYNTHESIZE in the pipeline. Scans:
  - Recent compile output (new/updated wiki pages)
  - New synthesis briefs
  - Active thesis pages
  - Active recommendations and predictions

Emits Update objects per agent_docs/update-schema.md. Each update is a JSON file at:
    updates/YYYY-MM-DD/{update-id}.json

Priority tier logic is deterministic. The "why this matters" body is Sonnet-written
with the wiki as cached context.

Update types emitted in this version:
  - synthesis           — wraps a new synthesis brief
  - thesis_pressure     — new evidence on an active thesis
  - prediction_resolved — a prediction's resolution date has passed

Future types (stubbed but not yet implemented):
  - convergence, contradiction, entity_shift, anomaly

Usage:
    python scripts/score.py                # Run a full scoring pass
    python scripts/score.py --dry-run      # Show what would be emitted
    python scripts/score.py --since 24h    # Limit to last 24 hours of compile output
    python scripts/score.py --type synthesis  # Only emit one type

Requires ANTHROPIC_API_KEY for the body generation (Sonnet).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from brain_io import append_log, brain_root, utcnow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 800  # Update bodies are short by design

# Confidence thresholds (from agent_docs/update-schema.md)
INBOX_CONFIDENCE_FLOOR = 75
FEED_CONFIDENCE_FLOOR = 40

# TTL map per update type (hours). null = never expires.
TYPE_TTL_HOURS: dict[str, int | None] = {
    "convergence": 14 * 24,
    "contradiction": 30 * 24,
    "thesis_pressure": 14 * 24,
    "entity_shift": 30 * 24,
    "prediction_resolved": None,  # permanent
    "anomaly": 72,
    "synthesis_intraday": 7 * 24,
    "synthesis_daily": 30 * 24,
    "synthesis_weekly": 90 * 24,
    "synthesis_monthly": None,  # permanent
    "synthesis_event": 14 * 24,
}

# Active synthesis subtypes for TTL routing
SYNTHESIS_TTL_MAP = {
    "intraday-brief": TYPE_TTL_HOURS["synthesis_intraday"],
    "daily-wrap": TYPE_TTL_HOURS["synthesis_daily"],
    "weekly-deep": TYPE_TTL_HOURS["synthesis_weekly"],
    "monthly-review": TYPE_TTL_HOURS["synthesis_monthly"],
    "event-driven": TYPE_TTL_HOURS["synthesis_event"],
}


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def updates_dir() -> Path:
    """Root directory for emitted updates."""
    return brain_root() / "updates"


def updates_dir_for_date(dt: datetime) -> Path:
    """Day-bucketed directory for a given timestamp."""
    return updates_dir() / dt.strftime("%Y-%m-%d")


def syntheses_dir() -> Path:
    return brain_root() / "wiki" / "syntheses"


def theses_dir() -> Path:
    return brain_root() / "wiki" / "theses"


def recommendations_dir() -> Path:
    return brain_root() / "wiki" / "recommendations"


def score_state_path() -> Path:
    """Track what we've already scored to avoid duplicate emits."""
    return updates_dir() / ".state"


# ---------------------------------------------------------------------------
# State tracking
# ---------------------------------------------------------------------------

def load_score_state() -> dict[str, Any]:
    """Load the score state (which artifacts have already been scored)."""
    path = score_state_path()
    if not path.exists():
        return {"synthesis_scored": [], "thesis_pressure_scored": [], "prediction_scored": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Corrupted score state — starting fresh")
        return {"synthesis_scored": [], "thesis_pressure_scored": [], "prediction_scored": []}


def save_score_state(state: dict[str, Any]) -> None:
    path = score_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


# ---------------------------------------------------------------------------
# Update emission
# ---------------------------------------------------------------------------

def compute_priority_tier(
    confidence_score: int,
    touches_active_thesis: bool,
    is_convergence_on_watched_entity: bool = False,
) -> str:
    """Deterministic tier computation per agent_docs/update-schema.md."""
    if confidence_score >= INBOX_CONFIDENCE_FLOOR and (
        touches_active_thesis or is_convergence_on_watched_entity
    ):
        return "inbox"
    if confidence_score >= FEED_CONFIDENCE_FLOOR:
        return "feed"
    return "archive"


def compute_expiration(update_type: str, created_at: datetime, subtype: str | None = None) -> str | None:
    """Compute expires_at ISO timestamp per the type→TTL map. Returns None if permanent."""
    if update_type == "synthesis" and subtype:
        ttl_hours = SYNTHESIS_TTL_MAP.get(subtype, TYPE_TTL_HOURS["synthesis_intraday"])
    else:
        ttl_hours = TYPE_TTL_HOURS.get(update_type)

    if ttl_hours is None:
        return None
    return (created_at + timedelta(hours=ttl_hours)).isoformat()


def emit_update(
    update_type: str,
    headline: str,
    body: str,
    affected_pages: list[str],
    affected_theses: list[str],
    source_evidence: list[str],
    confidence_score: int,
    *,
    subtype: str | None = None,
    recommendation: dict[str, Any] | None = None,
    actions: list[dict[str, str]] | None = None,
    is_convergence_on_watched_entity: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Construct and write an Update JSON file.

    Returns the update dict (whether or not it was written).
    """
    created_at = utcnow()
    touches_active = bool(affected_theses)
    tier = compute_priority_tier(confidence_score, touches_active, is_convergence_on_watched_entity)

    update = {
        "update_id": str(uuid.uuid4()),
        "type": update_type,
        "priority_tier": tier,
        "headline": headline[:140],
        "body": body[:1000],
        "affected_pages": affected_pages,
        "affected_theses": affected_theses,
        "source_evidence": source_evidence,
        "confidence_score": confidence_score,
        "recommendation": recommendation,
        "created_at": created_at.isoformat(),
        "expires_at": compute_expiration(update_type, created_at, subtype),
        "actions": actions or [{"label": "Dismiss", "action": "dismiss"}],
        "user_state": "unread",
    }

    if dry_run:
        logger.info(
            "[DRY RUN] Would emit %s (%s tier, confidence %d): %s",
            update_type, tier, confidence_score, headline[:80]
        )
        return update

    out_dir = updates_dir_for_date(created_at)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{update['update_id']}.json"
    out_path.write_text(json.dumps(update, indent=2, default=str), encoding="utf-8")
    logger.info("Emitted %s update at %s tier: %s", update_type, tier, headline[:80])

    return update


# ---------------------------------------------------------------------------
# Wiki page / synthesis parsing helpers
# ---------------------------------------------------------------------------

_FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)


def parse_frontmatter(content: str) -> dict[str, Any]:
    """Parse YAML frontmatter from a markdown file. Permissive parser."""
    m = _FRONTMATTER_RE.match(content)
    if not m:
        return {}

    fm: dict[str, Any] = {}
    for line in m.group(1).splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        if line.startswith("  ") or line.startswith("\t"):
            continue  # nested list/dict item
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        if not value:
            fm[key] = None
            continue
        if value.startswith("[") and value.endswith("]"):
            items = [v.strip().strip("\"'") for v in value[1:-1].split(",") if v.strip()]
            fm[key] = items
        elif value in ("true", "True"):
            fm[key] = True
        elif value in ("false", "False"):
            fm[key] = False
        else:
            try:
                fm[key] = float(value) if "." in value else int(value)
            except ValueError:
                fm[key] = value.strip("\"'")
    return fm


def list_synthesis_pages() -> list[Path]:
    """Return all synthesis pages, sorted by modification time."""
    d = syntheses_dir()
    if not d.exists():
        return []
    return sorted(d.glob("*.md"), key=lambda p: p.stat().st_mtime)


def list_thesis_pages() -> list[Path]:
    """Return all thesis pages."""
    d = theses_dir()
    if not d.exists():
        return []
    return sorted(d.glob("*.md"))


# ---------------------------------------------------------------------------
# Update type: synthesis (wraps a new synthesis brief)
# ---------------------------------------------------------------------------

def emit_synthesis_updates(state: dict[str, Any], dry_run: bool = False) -> list[dict[str, Any]]:
    """Wrap each new synthesis brief in a synthesis-type Update."""
    emitted: list[dict[str, Any]] = []
    already_scored = set(state.get("synthesis_scored", []))

    for synth_path in list_synthesis_pages():
        rel = str(synth_path.relative_to(brain_root()))
        if rel in already_scored:
            continue

        content = synth_path.read_text(encoding="utf-8")
        fm = parse_frontmatter(content)

        subtype = fm.get("subtype", "intraday-brief")
        title = fm.get("title", synth_path.stem)
        themes = fm.get("themes_covered", []) or []
        theses = fm.get("theses_covered", []) or []

        # Subtype-aware confidence floor — monthly/weekly are inherently higher signal
        confidence = {
            "monthly-review": 85,
            "weekly-deep": 80,
            "daily-wrap": 72,
            "event-driven": 78,
            "intraday-brief": 65,
        }.get(subtype, 65)

        # Headline pulled from synthesis title; body is a brief teaser
        headline = f"{subtype.replace('-', ' ').title()}: {title[:100]}"
        body = (
            f"New {subtype} synthesis available. "
            f"Covers {len(themes)} themes across {len(theses)} active theses. "
            f"Open the brief for full analysis."
        )

        update = emit_update(
            update_type="synthesis",
            headline=headline,
            body=body,
            affected_pages=[f"[[{t}]]" for t in themes[:5]],
            affected_theses=theses,
            source_evidence=[rel],
            confidence_score=confidence,
            subtype=subtype,
            actions=[
                {"label": "Open brief", "target": f"wiki://{rel}"},
                {"label": "Dismiss", "action": "dismiss"},
            ],
            dry_run=dry_run,
        )
        emitted.append(update)
        already_scored.add(rel)

    state["synthesis_scored"] = sorted(already_scored)
    return emitted


# ---------------------------------------------------------------------------
# Update type: thesis_pressure
# ---------------------------------------------------------------------------

def emit_thesis_pressure_updates(
    state: dict[str, Any],
    lookback_hours: int = 24,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Scan thesis pages for recently-added evidence and emit pressure updates.

    Heuristic v1: a thesis page whose mtime is within `lookback_hours` AND
    that has evidence entries newer than its last_scored mark gets a pressure update.

    More sophisticated detection (diffing supporting vs contradicting evidence,
    sentiment direction, source tier weighting) is future work.
    """
    emitted: list[dict[str, Any]] = []
    already_scored: dict[str, str] = state.get("thesis_pressure_scored_v2", {})

    cutoff = utcnow() - timedelta(hours=lookback_hours)

    for thesis_path in list_thesis_pages():
        rel = str(thesis_path.relative_to(brain_root()))
        mtime = datetime.fromtimestamp(thesis_path.stat().st_mtime, tz=timezone.utc)

        if mtime < cutoff:
            continue

        last_scored_ts = already_scored.get(rel)
        if last_scored_ts and datetime.fromisoformat(last_scored_ts) >= mtime:
            continue  # already scored this version

        content = thesis_path.read_text(encoding="utf-8")
        fm = parse_frontmatter(content)

        slug = fm.get("slug", thesis_path.stem)
        title = fm.get("title", slug)
        confidence_field = fm.get("confidence", 0.5)
        try:
            base_confidence = int(float(confidence_field) * 100)
        except (TypeError, ValueError):
            base_confidence = 60

        direction = fm.get("direction", "neutral")
        primary_assets = fm.get("primary_assets", []) or fm.get("primary_asset", [])
        if isinstance(primary_assets, str):
            primary_assets = [primary_assets]

        # Heuristic body — Sonnet enrichment is a TODO and will use prompt caching
        headline = f"Thesis pressure: {title}"
        body = (
            f"Wiki update detected on the {title} thesis. "
            f"Direction: {direction}. Primary assets: {', '.join(primary_assets[:5]) or 'none'}. "
            f"Open the thesis for current evidence and recommendations."
        )

        update = emit_update(
            update_type="thesis_pressure",
            headline=headline,
            body=body,
            affected_pages=[f"[[{title}]]"],
            affected_theses=[slug],
            source_evidence=[rel],
            confidence_score=base_confidence,
            actions=[
                {"label": "Open thesis", "target": f"wiki://{rel}"},
                {"label": "Dismiss", "action": "dismiss"},
            ],
            dry_run=dry_run,
        )
        emitted.append(update)
        already_scored[rel] = utcnow().isoformat()

    state["thesis_pressure_scored_v2"] = already_scored
    return emitted


# ---------------------------------------------------------------------------
# Update type: prediction_resolved
# ---------------------------------------------------------------------------

# Predictions table in thesis pages — markdown table rows with a date
_PREDICTION_ROW_RE = re.compile(
    r"^\|\s*(?P<prediction>[^|]+?)\s*\|\s*(?P<resolves>\d{4}-\d{2}-\d{2}|\d{4}-Q\d)\s*\|"
    r"\s*(?P<confidence>[0-9.]+)\s*\|\s*(?P<status>open|hit|miss|partial|resolved)\s*\|",
    re.IGNORECASE,
)


def extract_predictions_from_thesis(thesis_path: Path) -> list[dict[str, Any]]:
    """Parse the Predictions table from a thesis page.

    Returns list of {prediction, resolves, confidence, status, thesis_slug, thesis_title}.
    Permissive — silently skips malformed rows.
    """
    content = thesis_path.read_text(encoding="utf-8")
    fm = parse_frontmatter(content)
    slug = fm.get("slug", thesis_path.stem)
    title = fm.get("title", slug)

    rows: list[dict[str, Any]] = []
    in_predictions = False
    for line in content.splitlines():
        if line.strip().startswith("## Predictions"):
            in_predictions = True
            continue
        if in_predictions and line.startswith("## "):
            break
        if not in_predictions:
            continue
        m = _PREDICTION_ROW_RE.match(line)
        if not m:
            continue
        rows.append({
            "prediction": m.group("prediction").strip(),
            "resolves": m.group("resolves").strip(),
            "confidence": float(m.group("confidence")),
            "status": m.group("status").strip().lower(),
            "thesis_slug": slug,
            "thesis_title": title,
            "source_path": str(thesis_path.relative_to(brain_root())),
        })
    return rows


def _parse_resolves(resolves: str) -> datetime | None:
    """Convert YYYY-MM-DD or YYYY-Q# to a datetime."""
    try:
        if "Q" in resolves.upper():
            year, q = resolves.upper().split("-Q")
            quarter_end_month = int(q) * 3
            quarter_end_day = 31 if quarter_end_month in (3, 12) else 30
            return datetime(int(year), quarter_end_month, quarter_end_day, tzinfo=timezone.utc)
        return datetime.strptime(resolves, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None


def emit_prediction_resolved_updates(
    state: dict[str, Any],
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Emit prediction_resolved updates for predictions whose resolution date has passed."""
    emitted: list[dict[str, Any]] = []
    already_scored = set(state.get("prediction_scored", []))
    now = utcnow()

    for thesis_path in list_thesis_pages():
        for row in extract_predictions_from_thesis(thesis_path):
            if row["status"] != "open":
                continue

            resolves_dt = _parse_resolves(row["resolves"])
            if resolves_dt is None or resolves_dt > now:
                continue

            # Build a stable key for dedup
            key = f"{row['thesis_slug']}::{row['prediction'][:80]}::{row['resolves']}"
            if key in already_scored:
                continue

            headline = f"Prediction resolved: {row['prediction'][:100]}"
            body = (
                f"Stated {row['resolves']} on the {row['thesis_title']} thesis. "
                f"Confidence at issue: {row['confidence']:.2f}. "
                f"Status pending operator review — needs hit/miss/partial grading. "
                f"Brier contribution will be computed once outcome is recorded."
            )

            update = emit_update(
                update_type="prediction_resolved",
                headline=headline,
                body=body,
                affected_pages=[f"[[{row['thesis_title']}]]"],
                affected_theses=[row["thesis_slug"]],
                source_evidence=[row["source_path"]],
                confidence_score=85,  # always inbox — operator needs to grade
                actions=[
                    {"label": "Grade as hit", "action": "grade_hit"},
                    {"label": "Grade as miss", "action": "grade_miss"},
                    {"label": "Grade as partial", "action": "grade_partial"},
                    {"label": "Open thesis", "target": f"wiki://{row['source_path']}"},
                ],
                dry_run=dry_run,
            )
            emitted.append(update)
            already_scored.add(key)

    state["prediction_scored"] = sorted(already_scored)
    return emitted


# ---------------------------------------------------------------------------
# Auto-archive expired updates
# ---------------------------------------------------------------------------

def auto_archive_expired(dry_run: bool = False) -> int:
    """Walk updates/ and demote any update past expires_at to archive tier.

    Returns count of updates archived in this pass.
    """
    archived = 0
    now = utcnow()
    if not updates_dir().exists():
        return 0

    for json_path in updates_dir().rglob("*.json"):
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue

        if data.get("priority_tier") == "archive":
            continue
        expires_at = data.get("expires_at")
        if not expires_at:
            continue
        try:
            exp_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError:
            continue
        if exp_dt > now:
            continue

        data["priority_tier"] = "archive"
        if not dry_run:
            json_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        archived += 1

    if archived > 0 and not dry_run:
        logger.info("Auto-archived %d expired updates", archived)
    return archived


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def score_all(
    *,
    types: list[str] | None = None,
    lookback_hours: int = 24,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run a full scoring pass.

    Args:
        types: Which update types to emit. Defaults to all implemented types.
        lookback_hours: Window for thesis_pressure scans.
        dry_run: Show what would be emitted without writing.
    """
    enabled_types = set(types or ["synthesis", "thesis_pressure", "prediction_resolved"])

    state = load_score_state()
    totals = {"emitted_by_type": {}, "archived": 0}

    if "synthesis" in enabled_types:
        emitted = emit_synthesis_updates(state, dry_run=dry_run)
        totals["emitted_by_type"]["synthesis"] = len(emitted)

    if "thesis_pressure" in enabled_types:
        emitted = emit_thesis_pressure_updates(state, lookback_hours=lookback_hours, dry_run=dry_run)
        totals["emitted_by_type"]["thesis_pressure"] = len(emitted)

    if "prediction_resolved" in enabled_types:
        emitted = emit_prediction_resolved_updates(state, dry_run=dry_run)
        totals["emitted_by_type"]["prediction_resolved"] = len(emitted)

    totals["archived"] = auto_archive_expired(dry_run=dry_run)

    if not dry_run:
        save_score_state(state)
        total_emitted = sum(totals["emitted_by_type"].values())
        if total_emitted > 0:
            append_log(
                f"SCORE | emitted: {total_emitted} | "
                f"archived: {totals['archived']} | "
                f"breakdown: {totals['emitted_by_type']}"
            )

    return totals


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_lookback(value: str) -> int:
    """Accept '24h', '7d', '120m' style lookback strings; return hours."""
    value = value.strip().lower()
    if value.endswith("h"):
        return int(value[:-1])
    if value.endswith("d"):
        return int(value[:-1]) * 24
    if value.endswith("m"):
        return max(1, int(value[:-1]) // 60)
    return int(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SCORE stage")
    parser.add_argument(
        "--type",
        action="append",
        choices=["synthesis", "thesis_pressure", "prediction_resolved"],
        help="Restrict to specific update types (default: all)",
    )
    parser.add_argument("--since", default="24h", help="Lookback window for thesis_pressure (e.g. 24h, 7d)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be emitted")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    result = score_all(
        types=args.type,
        lookback_hours=_parse_lookback(args.since),
        dry_run=args.dry_run,
    )

    total = sum(result["emitted_by_type"].values())
    logger.info(
        "SCORE complete: %d emitted, %d archived, breakdown: %s",
        total, result["archived"], result["emitted_by_type"],
    )


if __name__ == "__main__":
    main()
