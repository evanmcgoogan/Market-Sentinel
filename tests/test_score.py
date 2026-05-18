"""Tests for score.py — SCORE stage that emits typed Stream Updates.

Tests cover: tier computation, TTL/expiration math, frontmatter parsing,
synthesis wrapping, thesis_pressure detection, prediction_resolved emission,
auto-archive, and state dedup.

No network calls; all data fixtures are written into tmp_brain.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import score


# ---------------------------------------------------------------------------
# Tier computation
# ---------------------------------------------------------------------------


class TestPriorityTier:
    def test_inbox_when_high_confidence_and_thesis(self):
        assert score.compute_priority_tier(80, touches_active_thesis=True) == "inbox"

    def test_feed_when_medium_confidence(self):
        assert score.compute_priority_tier(50, touches_active_thesis=True) == "feed"

    def test_archive_below_floor(self):
        assert score.compute_priority_tier(20, touches_active_thesis=True) == "archive"

    def test_high_confidence_no_thesis_demotes_to_feed(self):
        """≥75 confidence but no thesis match → feed, not inbox."""
        assert score.compute_priority_tier(80, touches_active_thesis=False) == "feed"

    def test_convergence_on_watched_entity_promotes(self):
        assert score.compute_priority_tier(
            80, touches_active_thesis=False, is_convergence_on_watched_entity=True
        ) == "inbox"

    def test_boundary_at_inbox_floor(self):
        assert score.compute_priority_tier(75, True) == "inbox"
        assert score.compute_priority_tier(74, True) == "feed"

    def test_boundary_at_feed_floor(self):
        assert score.compute_priority_tier(40, True) == "feed"
        assert score.compute_priority_tier(39, True) == "archive"


# ---------------------------------------------------------------------------
# Expiration math
# ---------------------------------------------------------------------------


class TestExpiration:
    def test_anomaly_72h_ttl(self):
        now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
        exp = score.compute_expiration("anomaly", now)
        assert exp == (now + timedelta(hours=72)).isoformat()

    def test_prediction_resolved_never_expires(self):
        now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
        assert score.compute_expiration("prediction_resolved", now) is None

    def test_synthesis_intraday_7d(self):
        now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
        exp = score.compute_expiration("synthesis", now, subtype="intraday-brief")
        assert exp == (now + timedelta(hours=7 * 24)).isoformat()

    def test_synthesis_monthly_never_expires(self):
        now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
        assert score.compute_expiration("synthesis", now, subtype="monthly-review") is None

    def test_synthesis_unknown_subtype_falls_back_to_intraday(self):
        now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
        exp = score.compute_expiration("synthesis", now, subtype="invented-subtype")
        # Falls back to intraday TTL
        assert exp == (now + timedelta(hours=7 * 24)).isoformat()


# ---------------------------------------------------------------------------
# Frontmatter parsing
# ---------------------------------------------------------------------------


class TestFrontmatter:
    def test_parses_simple(self):
        content = "---\ntitle: Hello\nstatus: active\n---\nbody"
        fm = score.parse_frontmatter(content)
        assert fm["title"] == "Hello"
        assert fm["status"] == "active"

    def test_parses_list(self):
        content = "---\ntags: [a, b, c]\n---\n"
        fm = score.parse_frontmatter(content)
        assert fm["tags"] == ["a", "b", "c"]

    def test_parses_numeric(self):
        content = "---\nconfidence: 0.78\ncount: 5\n---\n"
        fm = score.parse_frontmatter(content)
        assert fm["confidence"] == 0.78
        assert fm["count"] == 5

    def test_parses_bool(self):
        content = "---\nactive: true\nresolved: false\n---\n"
        fm = score.parse_frontmatter(content)
        assert fm["active"] is True
        assert fm["resolved"] is False

    def test_missing_frontmatter_returns_empty(self):
        assert score.parse_frontmatter("no frontmatter here") == {}

    def test_strips_quotes(self):
        content = '---\ntitle: "Quoted Title"\n---\n'
        fm = score.parse_frontmatter(content)
        assert fm["title"] == "Quoted Title"


# ---------------------------------------------------------------------------
# Emit update
# ---------------------------------------------------------------------------


class TestEmitUpdate:
    def test_dry_run_writes_nothing(self, tmp_brain):
        result = score.emit_update(
            update_type="synthesis",
            headline="Test",
            body="Test body",
            affected_pages=[],
            affected_theses=["test"],
            source_evidence=["raw/test.md"],
            confidence_score=80,
            subtype="intraday-brief",
            dry_run=True,
        )
        assert result["headline"] == "Test"
        # No files written
        assert not (tmp_brain / "updates").exists()

    def test_writes_to_day_directory(self, tmp_brain):
        score.emit_update(
            update_type="thesis_pressure",
            headline="Test pressure",
            body="Body",
            affected_pages=["[[X]]"],
            affected_theses=["robotics-era"],
            source_evidence=["wiki/theses/robotics-era.md"],
            confidence_score=72,
        )
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        day_dir = tmp_brain / "updates" / today
        assert day_dir.exists()
        files = list(day_dir.glob("*.json"))
        assert len(files) == 1

    def test_writes_valid_json(self, tmp_brain):
        score.emit_update(
            update_type="synthesis",
            headline="Daily wrap",
            body="Body",
            affected_pages=[],
            affected_theses=["intuition-as-edge"],
            source_evidence=["wiki/syntheses/x.md"],
            confidence_score=72,
            subtype="daily-wrap",
        )
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        files = list((tmp_brain / "updates" / today).glob("*.json"))
        data = json.loads(files[0].read_text())
        assert data["type"] == "synthesis"
        assert data["headline"] == "Daily wrap"
        assert data["confidence_score"] == 72
        assert "update_id" in data
        assert "created_at" in data
        assert "expires_at" in data
        assert data["user_state"] == "unread"

    def test_headline_truncated_to_140(self, tmp_brain):
        long_headline = "x" * 200
        update = score.emit_update(
            update_type="anomaly",
            headline=long_headline,
            body="Body",
            affected_pages=[],
            affected_theses=[],
            source_evidence=["raw/x.md"],
            confidence_score=50,
            dry_run=True,
        )
        assert len(update["headline"]) == 140

    def test_body_truncated_to_1000(self, tmp_brain):
        long_body = "x" * 1500
        update = score.emit_update(
            update_type="anomaly",
            headline="h",
            body=long_body,
            affected_pages=[],
            affected_theses=[],
            source_evidence=["raw/x.md"],
            confidence_score=50,
            dry_run=True,
        )
        assert len(update["body"]) == 1000


# ---------------------------------------------------------------------------
# Synthesis update wrapping
# ---------------------------------------------------------------------------


def _write_synthesis(tmp_brain: Path, name: str, subtype: str, themes: list[str], theses: list[str]) -> Path:
    syntheses = tmp_brain / "wiki" / "syntheses"
    syntheses.mkdir(parents=True, exist_ok=True)
    themes_str = "[" + ", ".join(themes) + "]"
    theses_str = "[" + ", ".join(theses) + "]"
    path = syntheses / name
    path.write_text(
        f"---\ntitle: Test Synthesis\nsubtype: {subtype}\n"
        f"themes_covered: {themes_str}\ntheses_covered: {theses_str}\n---\nbody",
        encoding="utf-8",
    )
    return path


class TestSynthesisUpdates:
    def test_wraps_new_synthesis(self, tmp_brain):
        _write_synthesis(tmp_brain, "2026-05-18--1400-intraday-brief.md", "intraday-brief",
                          ["ai-supercycle"], ["ai-infrastructure-supercycle"])
        state = {"synthesis_scored": []}
        emitted = score.emit_synthesis_updates(state, dry_run=False)
        assert len(emitted) == 1
        assert emitted[0]["type"] == "synthesis"

    def test_does_not_double_emit(self, tmp_brain):
        _write_synthesis(tmp_brain, "2026-05-18--1400-intraday-brief.md", "intraday-brief", [], [])
        state = {"synthesis_scored": []}
        score.emit_synthesis_updates(state, dry_run=False)
        # Second call should be no-op
        emitted_again = score.emit_synthesis_updates(state, dry_run=False)
        assert len(emitted_again) == 0

    def test_monthly_review_higher_confidence(self, tmp_brain):
        _write_synthesis(tmp_brain, "2026-05-04--0500-monthly-review.md",
                          "monthly-review", [], ["ai-infrastructure-supercycle"])
        state = {"synthesis_scored": []}
        emitted = score.emit_synthesis_updates(state, dry_run=True)
        assert emitted[0]["confidence_score"] == 85
        assert emitted[0]["priority_tier"] == "inbox"

    def test_intraday_lower_confidence(self, tmp_brain):
        _write_synthesis(tmp_brain, "2026-05-18--1000-intraday-brief.md",
                          "intraday-brief", [], ["robotics-era"])
        state = {"synthesis_scored": []}
        emitted = score.emit_synthesis_updates(state, dry_run=True)
        assert emitted[0]["confidence_score"] == 65
        assert emitted[0]["priority_tier"] == "feed"


# ---------------------------------------------------------------------------
# Thesis pressure updates
# ---------------------------------------------------------------------------


def _write_thesis(tmp_brain: Path, slug: str, confidence: float = 0.72) -> Path:
    theses = tmp_brain / "wiki" / "theses"
    theses.mkdir(parents=True, exist_ok=True)
    path = theses / f"{slug}.md"
    path.write_text(
        f"---\ntitle: Test Thesis {slug}\nslug: {slug}\nstatus: active\n"
        f"confidence: {confidence}\ndirection: long\nprimary_assets: [MU, NVDA]\n---\n"
        f"body content\n\n## Predictions\n\n| Prediction | Resolves | Confidence | Status |\n",
        encoding="utf-8",
    )
    return path


class TestThesisPressureUpdates:
    def test_emits_for_recent_update(self, tmp_brain):
        _write_thesis(tmp_brain, "ai-infrastructure-supercycle", confidence=0.82)
        state = {}
        emitted = score.emit_thesis_pressure_updates(state, lookback_hours=24)
        assert len(emitted) >= 1
        assert any(u["type"] == "thesis_pressure" for u in emitted)

    def test_skips_old_thesis(self, tmp_brain):
        path = _write_thesis(tmp_brain, "old-thesis", confidence=0.5)
        # Backdate to 100 hours ago
        old_ts = time.time() - (100 * 3600)
        import os as _os
        _os.utime(path, (old_ts, old_ts))
        state = {}
        emitted = score.emit_thesis_pressure_updates(state, lookback_hours=24)
        assert all(u["affected_theses"] != ["old-thesis"] for u in emitted)

    def test_does_not_re_emit_unchanged(self, tmp_brain):
        _write_thesis(tmp_brain, "robotics-era", confidence=0.78)
        state = {}
        emitted_first = score.emit_thesis_pressure_updates(state, lookback_hours=24)
        emitted_second = score.emit_thesis_pressure_updates(state, lookback_hours=24)
        assert len(emitted_first) >= 1
        assert len(emitted_second) == 0


# ---------------------------------------------------------------------------
# Prediction extraction
# ---------------------------------------------------------------------------


class TestPredictionExtraction:
    def test_extracts_resolved_passed_predictions(self, tmp_brain):
        theses = tmp_brain / "wiki" / "theses"
        theses.mkdir(parents=True, exist_ok=True)
        (theses / "test-thesis.md").write_text(
            "---\ntitle: Test Thesis\nslug: test-thesis\nstatus: active\nconfidence: 0.7\n---\n\n"
            "## Predictions\n\n"
            "| Prediction | Resolves | Confidence | Status |\n"
            "|---|---|---|---|\n"
            "| Past prediction A | 2025-01-01 | 0.65 | open |\n"
            "| Future prediction B | 2030-01-01 | 0.80 | open |\n",
            encoding="utf-8",
        )
        state = {}
        emitted = score.emit_prediction_resolved_updates(state)
        # Only the past one should be emitted
        assert len(emitted) == 1
        assert "Past prediction A" in emitted[0]["headline"]

    def test_skips_already_scored(self, tmp_brain):
        theses = tmp_brain / "wiki" / "theses"
        theses.mkdir(parents=True, exist_ok=True)
        (theses / "test.md").write_text(
            "---\ntitle: T\nslug: test\nstatus: active\nconfidence: 0.7\n---\n\n"
            "## Predictions\n\n"
            "| Prediction | Resolves | Confidence | Status |\n"
            "|---|---|---|---|\n"
            "| Past prediction | 2025-01-01 | 0.7 | open |\n",
            encoding="utf-8",
        )
        state = {}
        score.emit_prediction_resolved_updates(state)
        emitted_again = score.emit_prediction_resolved_updates(state)
        assert len(emitted_again) == 0

    def test_quarter_resolves_parses(self):
        dt = score._parse_resolves("2025-Q1")
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 3

    def test_malformed_resolves_returns_none(self):
        assert score._parse_resolves("not-a-date") is None

    def test_prediction_resolved_is_always_inbox(self, tmp_brain):
        theses = tmp_brain / "wiki" / "theses"
        theses.mkdir(parents=True, exist_ok=True)
        (theses / "t.md").write_text(
            "---\ntitle: T\nslug: t\nstatus: active\nconfidence: 0.5\n---\n\n"
            "## Predictions\n\n"
            "| Prediction | Resolves | Confidence | Status |\n"
            "|---|---|---|---|\n"
            "| Past prediction | 2024-01-01 | 0.6 | open |\n",
            encoding="utf-8",
        )
        state = {}
        emitted = score.emit_prediction_resolved_updates(state, dry_run=True)
        assert emitted[0]["priority_tier"] == "inbox"


# ---------------------------------------------------------------------------
# Auto-archive
# ---------------------------------------------------------------------------


class TestAutoArchive:
    def test_archives_expired_updates(self, tmp_brain):
        # Write an expired update
        day = datetime(2025, 1, 1, tzinfo=timezone.utc)
        out_dir = tmp_brain / "updates" / day.strftime("%Y-%m-%d")
        out_dir.mkdir(parents=True, exist_ok=True)
        expired = {
            "update_id": "test-uuid",
            "type": "anomaly",
            "priority_tier": "feed",
            "headline": "test",
            "body": "test",
            "affected_pages": [],
            "affected_theses": [],
            "source_evidence": ["raw/x.md"],
            "confidence_score": 50,
            "recommendation": None,
            "created_at": day.isoformat(),
            "expires_at": (day + timedelta(hours=72)).isoformat(),
            "actions": [],
            "user_state": "unread",
        }
        (out_dir / "test-uuid.json").write_text(json.dumps(expired))

        archived = score.auto_archive_expired()
        assert archived == 1

        data = json.loads((out_dir / "test-uuid.json").read_text())
        assert data["priority_tier"] == "archive"

    def test_does_not_archive_fresh(self, tmp_brain):
        future = datetime.now(timezone.utc) + timedelta(days=10)
        out_dir = tmp_brain / "updates" / "2026-05-18"
        out_dir.mkdir(parents=True, exist_ok=True)
        fresh = {
            "update_id": "fresh-uuid",
            "priority_tier": "feed",
            "expires_at": future.isoformat(),
            "type": "synthesis",
            "headline": "h", "body": "b",
            "affected_pages": [], "affected_theses": [], "source_evidence": [],
            "confidence_score": 60, "recommendation": None,
            "created_at": "2026-05-18T12:00:00+00:00",
            "actions": [], "user_state": "unread",
        }
        (out_dir / "fresh-uuid.json").write_text(json.dumps(fresh))

        archived = score.auto_archive_expired()
        assert archived == 0

    def test_does_not_archive_permanent(self, tmp_brain):
        out_dir = tmp_brain / "updates" / "2024-01-01"
        out_dir.mkdir(parents=True, exist_ok=True)
        permanent = {
            "update_id": "permanent-uuid",
            "priority_tier": "feed",
            "expires_at": None,
            "type": "prediction_resolved",
            "headline": "h", "body": "b",
            "affected_pages": [], "affected_theses": [], "source_evidence": [],
            "confidence_score": 85, "recommendation": None,
            "created_at": "2024-01-01T12:00:00+00:00",
            "actions": [], "user_state": "unread",
        }
        (out_dir / "permanent-uuid.json").write_text(json.dumps(permanent))

        archived = score.auto_archive_expired()
        assert archived == 0


# ---------------------------------------------------------------------------
# Lookback parsing
# ---------------------------------------------------------------------------


class TestLookbackParse:
    def test_hours(self):
        assert score._parse_lookback("24h") == 24

    def test_days(self):
        assert score._parse_lookback("7d") == 168

    def test_minutes(self):
        assert score._parse_lookback("120m") == 2

    def test_plain_number(self):
        assert score._parse_lookback("48") == 48


# ---------------------------------------------------------------------------
# score_all orchestrator
# ---------------------------------------------------------------------------


class TestScoreAll:
    def test_runs_all_types(self, tmp_brain):
        _write_thesis(tmp_brain, "ai-infrastructure-supercycle")
        _write_synthesis(tmp_brain, "2026-05-18--1400-intraday-brief.md",
                          "intraday-brief", [], ["ai-infrastructure-supercycle"])
        result = score.score_all()
        assert "emitted_by_type" in result
        assert "synthesis" in result["emitted_by_type"]
        assert "thesis_pressure" in result["emitted_by_type"]
        assert "prediction_resolved" in result["emitted_by_type"]

    def test_type_filter_limits_to_one(self, tmp_brain):
        _write_thesis(tmp_brain, "robotics-era")
        result = score.score_all(types=["thesis_pressure"])
        assert "thesis_pressure" in result["emitted_by_type"]
        assert "synthesis" not in result["emitted_by_type"]

    def test_dry_run_writes_no_files(self, tmp_brain):
        _write_thesis(tmp_brain, "robotics-era")
        _write_synthesis(tmp_brain, "2026-05-18--1400-intraday-brief.md",
                          "intraday-brief", [], ["robotics-era"])
        score.score_all(dry_run=True)
        # No files in updates/
        if (tmp_brain / "updates").exists():
            files = list((tmp_brain / "updates").rglob("*.json"))
            assert len(files) == 0
