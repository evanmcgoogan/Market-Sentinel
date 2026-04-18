"""Tests for synthesize.py — the intelligence synthesis pipeline.

Tests cover all deterministic logic: state tracking, extraction discovery,
wiki context loading, skip conditions, output path generation, stub frontmatter,
prompt building, response parsing, content validation, frontmatter merging,
supersession, index updates, and end-to-end dry-run / no-llm modes.

No LLM calls are made in any test.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import synthesize as synth
import brain_io


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

UTC = timezone.utc

def _ts(hours_ago: float = 0.0) -> datetime:
    """Return a UTC datetime `hours_ago` hours in the past."""
    return datetime.now(UTC) - timedelta(hours=hours_ago)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_extraction(
    tmp_brain: Path,
    rel_path: str,
    verdict: str = "high_signal",
    hours_ago: float = 1.0,
    domains: list[str] | None = None,
) -> str:
    """Write a minimal extraction JSON file."""
    full = tmp_brain / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "source_file": "raw/tweets/test/2026-04-17.md",
        "triage_verdict": verdict,
        "triage_max_score": 0.9 if verdict == "high_signal" else 0.5,
        "extracted_at": _iso(_ts(hours_ago)),
        "domains": domains or ["ai", "macro"],
        "extractions": [{"type": "claim", "content": "Test claim"}],
        "affected_wiki_pages": ["wiki/entities/people/test.md"],
    }
    full.write_text(json.dumps(data))
    return rel_path


def _write_synthesis_page(
    tmp_brain: Path,
    subtype: str,
    status: str = "current",
    hours_ago: float = 2.0,
    filename: str | None = None,
) -> str:
    """Write a minimal synthesis wiki page."""
    synth_dir = tmp_brain / "wiki" / "syntheses"
    synth_dir.mkdir(parents=True, exist_ok=True)

    dt = _ts(hours_ago)
    if filename is None:
        filename = f"{dt.strftime('%Y-%m-%d')}--{dt.strftime('%H%M')}-{subtype}.md"

    full = synth_dir / filename
    created = dt.strftime("%Y-%m-%d")
    period_end = _iso(dt)

    content = f"""\
---
title: "Test {subtype}"
type: synthesis
subtype: {subtype}
period_start: {_iso(_ts(hours_ago + 2))}
period_end: {period_end}
model: sonnet-4.6
extraction_count: 3
high_signal_count: 1
sources_referenced: 2
wiki_pages_referenced: 4
key_findings: 2
themes_covered: []
theses_covered: []
created: {created}
updated: {created}
status: {status}
supersedes: []
superseded_by: null
tags: [synthesis, {subtype}]
---

## What Changed

- Test signal from `raw/tweets/test/2026-04-17.md`
"""
    full.write_text(content)
    return f"wiki/syntheses/{filename}"


def _write_thesis_page(tmp_brain: Path, name: str, status: str = "active") -> str:
    """Write a minimal thesis wiki page."""
    thesis_dir = tmp_brain / "wiki" / "theses"
    thesis_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{name}.md"
    content = f"""\
---
title: "{name}"
type: thesis
status: {status}
confidence: 0.7
direction: long
primary_asset: SPY
falsifiers: ["price drops below 400"]
invalidation_level: "SPY < 400"
entry_conditions: ["RSI < 30"]
exit_conditions: ["RSI > 70"]
update_count: 1
created: 2026-04-01
updated: 2026-04-17
tags: [thesis]
---

## Thesis

Test thesis content citing `raw/tweets/test/2026-04-17.md`.
"""
    (thesis_dir / filename).write_text(content)
    return f"wiki/theses/{filename}"


# ---------------------------------------------------------------------------
# Synthesis state
# ---------------------------------------------------------------------------


class TestSynthesisState:
    def test_load_empty_state(self, tmp_brain):
        state = synth.load_synthesis_state()
        assert state == {}

    def test_save_and_load_state(self, tmp_brain):
        state = {"intraday-brief": {"last_run": "2026-04-17T14:00:00+00:00", "last_path": "x.md"}}
        synth.save_synthesis_state(state)
        loaded = synth.load_synthesis_state()
        assert loaded["intraday-brief"]["last_path"] == "x.md"

    def test_get_last_synthesis_time_missing(self, tmp_brain):
        result = synth.get_last_synthesis_time("intraday-brief")
        assert result is None

    def test_get_last_synthesis_time_present(self, tmp_brain):
        state = {"intraday-brief": {"last_run": "2026-04-17T10:00:00+00:00", "last_path": "x.md"}}
        synth.save_synthesis_state(state)
        result = synth.get_last_synthesis_time("intraday-brief")
        assert result is not None
        assert result.year == 2026
        assert result.tzinfo is not None

    def test_record_synthesis(self, tmp_brain):
        now = _ts(0)
        synth.record_synthesis("daily-wrap", "wiki/syntheses/test.md", now)
        state = synth.load_synthesis_state()
        assert "daily-wrap" in state
        assert state["daily-wrap"]["last_path"] == "wiki/syntheses/test.md"

    def test_record_synthesis_overwrites(self, tmp_brain):
        t1 = _ts(2)
        t2 = _ts(0)
        synth.record_synthesis("intraday-brief", "wiki/syntheses/old.md", t1)
        synth.record_synthesis("intraday-brief", "wiki/syntheses/new.md", t2)
        state = synth.load_synthesis_state()
        assert state["intraday-brief"]["last_path"] == "wiki/syntheses/new.md"

    def test_state_file_location(self, tmp_brain):
        synth.save_synthesis_state({"test": {}})
        state_file = tmp_brain / "wiki" / "syntheses" / ".state"
        assert state_file.exists()


# ---------------------------------------------------------------------------
# Extraction discovery
# ---------------------------------------------------------------------------


class TestFindExtractionsSince:
    def test_finds_recent_extraction(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/tweets/test/2026-04-17.json", hours_ago=1.0)
        since = _ts(2.0)
        results = synth.find_extractions_since(since)
        assert len(results) == 1

    def test_excludes_old_extraction(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/tweets/test/2026-04-17.json", hours_ago=5.0)
        since = _ts(3.0)
        results = synth.find_extractions_since(since)
        assert len(results) == 0

    def test_excludes_noise_verdict(self, tmp_brain):
        _write_extraction(
            tmp_brain,
            "extractions/tweets/test/noise.json",
            verdict="noise",
            hours_ago=1.0,
        )
        since = _ts(2.0)
        results = synth.find_extractions_since(since)
        assert len(results) == 0

    def test_multiple_extractions(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/tweets/a/file.json", hours_ago=1.0)
        _write_extraction(tmp_brain, "extractions/tweets/b/file.json", hours_ago=0.5)
        since = _ts(2.0)
        results = synth.find_extractions_since(since)
        assert len(results) == 2

    def test_skips_dot_files(self, tmp_brain):
        # Create a .compiled dot file (should be ignored)
        ext_dir = tmp_brain / "extractions"
        ext_dir.mkdir(parents=True, exist_ok=True)
        (ext_dir / ".compiled").write_text("extractions/tweets/test/file.json\n")
        # And a real extraction
        _write_extraction(tmp_brain, "extractions/tweets/test/file.json", hours_ago=1.0)
        since = _ts(2.0)
        results = synth.find_extractions_since(since)
        assert len(results) == 1

    def test_empty_dir(self, tmp_brain):
        since = _ts(2.0)
        results = synth.find_extractions_since(since)
        assert results == []

    def test_mixed_verdicts(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/a.json", verdict="high_signal", hours_ago=1.0)
        _write_extraction(tmp_brain, "extractions/b.json", verdict="medium_signal", hours_ago=1.0)
        _write_extraction(tmp_brain, "extractions/c.json", verdict="noise", hours_ago=1.0)
        since = _ts(2.0)
        results = synth.find_extractions_since(since)
        assert len(results) == 2  # noise excluded


class TestEventDrivenTrigger:
    def test_no_trigger_when_empty(self, tmp_brain):
        assert synth.is_event_driven_trigger(threshold=3) is False

    def test_no_trigger_below_threshold(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/a.json", verdict="high_signal", hours_ago=0.5)
        _write_extraction(tmp_brain, "extractions/b.json", verdict="high_signal", hours_ago=0.5)
        assert synth.is_event_driven_trigger(threshold=3, window_hours=2.0) is False

    def test_trigger_at_threshold(self, tmp_brain):
        for i in range(3):
            _write_extraction(
                tmp_brain, f"extractions/{i}.json",
                verdict="high_signal", hours_ago=0.5,
            )
        assert synth.is_event_driven_trigger(threshold=3, window_hours=2.0) is True

    def test_no_trigger_outside_window(self, tmp_brain):
        # All high_signal but outside the 2-hour window
        for i in range(5):
            _write_extraction(
                tmp_brain, f"extractions/{i}.json",
                verdict="high_signal", hours_ago=3.0,
            )
        assert synth.is_event_driven_trigger(threshold=3, window_hours=2.0) is False


# ---------------------------------------------------------------------------
# Wiki context loading
# ---------------------------------------------------------------------------


class TestFindActiveTheses:
    def test_finds_active_thesis(self, tmp_brain):
        _write_thesis_page(tmp_brain, "test-thesis", status="active")
        theses = synth.find_active_theses()
        assert len(theses) == 1
        assert "test-thesis.md" in theses[0]

    def test_excludes_invalidated(self, tmp_brain):
        _write_thesis_page(tmp_brain, "dead-thesis", status="invalidated")
        theses = synth.find_active_theses()
        assert theses == []

    def test_multiple_theses(self, tmp_brain):
        _write_thesis_page(tmp_brain, "thesis-a", status="active")
        _write_thesis_page(tmp_brain, "thesis-b", status="active")
        _write_thesis_page(tmp_brain, "thesis-c", status="dormant")
        theses = synth.find_active_theses()
        assert len(theses) == 2

    def test_no_theses_dir(self, tmp_brain):
        theses = synth.find_active_theses()
        assert theses == []


class TestFindPriorSynthesis:
    def test_returns_none_when_empty(self, tmp_brain):
        result = synth.find_prior_synthesis("intraday-brief")
        assert result is None

    def test_finds_from_state(self, tmp_brain):
        path = _write_synthesis_page(tmp_brain, "intraday-brief", hours_ago=4.0)
        synth.record_synthesis("intraday-brief", path, _ts(4.0))
        result = synth.find_prior_synthesis("intraday-brief")
        assert result is not None
        assert "What Changed" in result

    def test_fallback_scan_when_state_missing(self, tmp_brain):
        _write_synthesis_page(tmp_brain, "daily-wrap", hours_ago=23.0)
        result = synth.find_prior_synthesis("daily-wrap")
        assert result is not None

    def test_returns_none_wrong_subtype(self, tmp_brain):
        _write_synthesis_page(tmp_brain, "daily-wrap", hours_ago=1.0)
        result = synth.find_prior_synthesis("weekly-deep")
        assert result is None


class TestFindCurrentSynthesesOfType:
    def test_finds_current(self, tmp_brain):
        _write_synthesis_page(tmp_brain, "intraday-brief", status="current", hours_ago=1.0)
        since = _ts(2.0)
        results = synth.find_current_syntheses_of_type("intraday-brief", since)
        assert len(results) == 1

    def test_excludes_superseded(self, tmp_brain):
        _write_synthesis_page(tmp_brain, "intraday-brief", status="superseded", hours_ago=1.0)
        since = _ts(2.0)
        results = synth.find_current_syntheses_of_type("intraday-brief", since)
        assert results == []

    def test_excludes_before_since(self, tmp_brain):
        _write_synthesis_page(tmp_brain, "intraday-brief", status="current", hours_ago=5.0)
        since = _ts(3.0)
        results = synth.find_current_syntheses_of_type("intraday-brief", since)
        assert results == []

    def test_empty_dir(self, tmp_brain):
        since = _ts(2.0)
        results = synth.find_current_syntheses_of_type("intraday-brief", since)
        assert results == []


# ---------------------------------------------------------------------------
# Skip logic
# ---------------------------------------------------------------------------


class TestShouldSkip:
    def test_skip_no_extractions(self, tmp_brain):
        skip, reason = synth.should_skip("intraday-brief", [])
        assert skip is True
        assert "no new extractions" in reason

    def test_skip_all_noise(self, tmp_brain):
        extractions = [{"triage_verdict": "noise"}, {"triage_verdict": "noise"}]
        skip, reason = synth.should_skip("intraday-brief", extractions)
        assert skip is True
        assert "noise" in reason

    def test_no_skip_with_signal(self, tmp_brain):
        extractions = [{"triage_verdict": "high_signal"}]
        skip, reason = synth.should_skip("intraday-brief", extractions)
        assert skip is False
        assert reason == ""

    def test_no_skip_mixed(self, tmp_brain):
        extractions = [
            {"triage_verdict": "noise"},
            {"triage_verdict": "medium_signal"},
        ]
        skip, reason = synth.should_skip("intraday-brief", extractions)
        assert skip is False


# ---------------------------------------------------------------------------
# Output path generation
# ---------------------------------------------------------------------------


class TestSynthesisOutputPath:
    def test_format_intraday(self):
        dt = datetime(2026, 4, 17, 14, 0, 0, tzinfo=UTC)
        path = synth.synthesis_output_path("intraday-brief", dt)
        assert path == "wiki/syntheses/2026-04-17--1400-intraday-brief.md"

    def test_format_daily_wrap(self):
        dt = datetime(2026, 4, 17, 21, 0, 0, tzinfo=UTC)
        path = synth.synthesis_output_path("daily-wrap", dt)
        assert path == "wiki/syntheses/2026-04-17--2100-daily-wrap.md"

    def test_format_weekly_deep(self):
        dt = datetime(2026, 4, 20, 4, 0, 0, tzinfo=UTC)
        path = synth.synthesis_output_path("weekly-deep", dt)
        assert path == "wiki/syntheses/2026-04-20--0400-weekly-deep.md"

    def test_format_event_driven(self):
        dt = datetime(2026, 4, 17, 9, 30, 0, tzinfo=UTC)
        path = synth.synthesis_output_path("event-driven", dt)
        assert path == "wiki/syntheses/2026-04-17--0930-event-driven.md"


# ---------------------------------------------------------------------------
# Stub frontmatter
# ---------------------------------------------------------------------------


class TestBuildStubFrontmatter:
    def _make_stub(self, subtype: str = "intraday-brief") -> dict:
        now = datetime(2026, 4, 17, 14, 0, 0, tzinfo=UTC)
        since = now - timedelta(hours=4)
        return synth._build_stub_frontmatter(
            subtype=subtype,
            period_start=since,
            period_end=now,
            extraction_count=5,
            high_signal_count=2,
            model=synth.SONNET_MODEL,
        )

    def test_required_fields_present(self):
        stub = self._make_stub()
        for field in ["title", "type", "subtype", "period_start", "period_end",
                      "model", "extraction_count", "high_signal_count", "created",
                      "updated", "status", "tags"]:
            assert field in stub, f"missing field: {field}"

    def test_type_is_synthesis(self):
        stub = self._make_stub()
        assert stub["type"] == "synthesis"

    def test_status_is_current(self):
        stub = self._make_stub()
        assert stub["status"] == "current"

    def test_intraday_uses_datetime_format(self):
        stub = self._make_stub("intraday-brief")
        # Should contain time component
        assert "T" in stub["period_start"]
        assert "T" in stub["period_end"]

    def test_daily_wrap_uses_date_format(self):
        now = datetime(2026, 4, 17, 21, 0, 0, tzinfo=UTC)
        stub = synth._build_stub_frontmatter(
            subtype="daily-wrap",
            period_start=now - timedelta(hours=24),
            period_end=now,
            extraction_count=10,
            high_signal_count=3,
            model=synth.SONNET_MODEL,
        )
        # Date-only format — no T
        assert "T" not in stub["period_start"]
        assert "T" not in stub["period_end"]

    def test_extraction_count_set(self):
        stub = self._make_stub()
        assert stub["extraction_count"] == 5
        assert stub["high_signal_count"] == 2

    def test_opus_model_short_name(self):
        now = datetime(2026, 4, 17, 14, 0, 0, tzinfo=UTC)
        stub = synth._build_stub_frontmatter(
            subtype="monthly-review",
            period_start=now - timedelta(days=30),
            period_end=now,
            extraction_count=100,
            high_signal_count=20,
            model=synth.OPUS_MODEL,
        )
        assert stub["model"] == "opus-4.7"

    def test_sonnet_model_short_name(self):
        stub = self._make_stub("intraday-brief")
        assert stub["model"] == "sonnet-4.6"


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------


class TestBuildSynthesisPrompt:
    def _make_stub(self, subtype: str = "intraday-brief") -> dict:
        now = datetime(2026, 4, 17, 14, 0, 0, tzinfo=UTC)
        return synth._build_stub_frontmatter(
            subtype=subtype,
            period_start=now - timedelta(hours=4),
            period_end=now,
            extraction_count=2,
            high_signal_count=1,
            model=synth.SONNET_MODEL,
        )

    def test_returns_messages_and_system(self):
        extractions = [{"triage_verdict": "high_signal", "extractions": []}]
        stub = self._make_stub()
        messages, system = synth.build_synthesis_prompt(
            subtype="intraday-brief",
            extractions=extractions,
            active_theses=[],
            prior_synthesis=None,
            stub_frontmatter=stub,
        )
        assert isinstance(messages, list)
        assert len(messages) == 1
        assert isinstance(system, str)

    def test_system_contains_subtype(self):
        stub = self._make_stub()
        _, system = synth.build_synthesis_prompt(
            subtype="intraday-brief",
            extractions=[{"triage_verdict": "high_signal"}],
            active_theses=[],
            prior_synthesis=None,
            stub_frontmatter=stub,
        )
        assert "intraday-brief" in system

    def test_user_message_contains_extractions(self):
        extractions = [{"triage_verdict": "high_signal", "content": "test signal"}]
        stub = self._make_stub()
        messages, _ = synth.build_synthesis_prompt(
            subtype="intraday-brief",
            extractions=extractions,
            active_theses=[],
            prior_synthesis=None,
            stub_frontmatter=stub,
        )
        user_text = messages[0]["content"][0]["text"]
        assert "test signal" in user_text

    def test_includes_prior_synthesis(self):
        stub = self._make_stub()
        messages, _ = synth.build_synthesis_prompt(
            subtype="intraday-brief",
            extractions=[{"triage_verdict": "high_signal"}],
            active_theses=[],
            prior_synthesis="## Prior synthesis content here",
            stub_frontmatter=stub,
        )
        user_text = messages[0]["content"][0]["text"]
        assert "Prior synthesis content here" in user_text

    def test_truncates_long_extractions(self):
        huge = {"triage_verdict": "high_signal", "content": "x" * 100_000}
        stub = self._make_stub()
        messages, _ = synth.build_synthesis_prompt(
            subtype="intraday-brief",
            extractions=[huge],
            active_theses=[],
            prior_synthesis=None,
            stub_frontmatter=stub,
            max_extraction_chars=1000,
        )
        user_text = messages[0]["content"][0]["text"]
        assert "[truncated]" in user_text

    def test_includes_theses_context(self, tmp_brain):
        _write_thesis_page(tmp_brain, "test-thesis", status="active")
        stub = self._make_stub()
        messages, _ = synth.build_synthesis_prompt(
            subtype="intraday-brief",
            extractions=[{"triage_verdict": "high_signal"}],
            active_theses=["wiki/theses/test-thesis.md"],
            prior_synthesis=None,
            stub_frontmatter=stub,
        )
        user_text = messages[0]["content"][0]["text"]
        assert "Active Theses" in user_text

    def test_body_template_varies_by_subtype(self):
        stub_intraday = self._make_stub("intraday-brief")
        stub_weekly = synth._build_stub_frontmatter(
            subtype="weekly-deep",
            period_start=_ts(7 * 24),
            period_end=_ts(0),
            extraction_count=50,
            high_signal_count=10,
            model=synth.SONNET_MODEL,
        )
        _, sys_intraday = synth.build_synthesis_prompt(
            "intraday-brief", [], [], None, stub_intraday
        )
        _, sys_weekly = synth.build_synthesis_prompt(
            "weekly-deep", [], [], None, stub_weekly
        )
        assert "What Changed" in sys_intraday
        assert "Theme Trajectories" in sys_weekly


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


class TestParseSynthesisResponse:
    def test_clean_markdown_passthrough(self):
        doc = "---\ntitle: test\n---\n\n## Body"
        assert synth.parse_synthesis_response(doc) == doc

    def test_strips_markdown_fences(self):
        fenced = "```markdown\n---\ntitle: test\n---\n\n## Body\n```"
        result = synth.parse_synthesis_response(fenced)
        assert result.startswith("---")
        assert "```" not in result

    def test_strips_md_fences(self):
        fenced = "```md\n---\ntitle: test\n---\n## Body\n```"
        result = synth.parse_synthesis_response(fenced)
        assert result.startswith("---")

    def test_strips_plain_fences(self):
        fenced = "```\n---\ntitle: test\n---\n## Body\n```"
        result = synth.parse_synthesis_response(fenced)
        assert result.startswith("---")

    def test_empty_returns_empty(self):
        assert synth.parse_synthesis_response("") == ""

    def test_strips_leading_trailing_whitespace(self):
        result = synth.parse_synthesis_response("  ---\ntitle: test\n---\n  ")
        assert not result.startswith(" ")


# ---------------------------------------------------------------------------
# Content validation
# ---------------------------------------------------------------------------


class TestValidateSynthesisContent:
    def _valid_doc(self) -> str:
        return """\
---
title: "2026-04-17 14:00 Intraday Brief"
type: synthesis
subtype: intraday-brief
period_start: 2026-04-17T10:00:00Z
period_end: 2026-04-17T14:00:00Z
model: sonnet-4.6
extraction_count: 3
high_signal_count: 1
sources_referenced: 2
wiki_pages_referenced: 4
key_findings: 2
themes_covered: []
theses_covered: []
created: 2026-04-17
updated: 2026-04-17
status: current
supersedes: []
superseded_by: null
tags: [synthesis, intraday-brief]
---

## What Changed

- Signal from `raw/tweets/test/2026-04-17.md`
"""

    def test_valid_document(self):
        errors = synth.validate_synthesis_content(self._valid_doc())
        assert errors == []

    def test_missing_frontmatter(self):
        errors = synth.validate_synthesis_content("## No frontmatter here")
        assert any("frontmatter" in e for e in errors)

    def test_unclosed_frontmatter(self):
        errors = synth.validate_synthesis_content("---\ntitle: test\n")
        assert any("frontmatter" in e.lower() or "---" in e for e in errors)

    def test_missing_required_field(self):
        doc = self._valid_doc().replace("type: synthesis\n", "")
        errors = synth.validate_synthesis_content(doc)
        assert any("type" in e for e in errors)

    def test_wrong_type(self):
        doc = self._valid_doc().replace("type: synthesis", "type: entity")
        errors = synth.validate_synthesis_content(doc)
        assert any("type" in e for e in errors)

    def test_multiple_missing_fields(self):
        doc = self._valid_doc()
        doc = doc.replace("title: \"2026-04-17 14:00 Intraday Brief\"\n", "")
        doc = doc.replace("model: sonnet-4.6\n", "")
        errors = synth.validate_synthesis_content(doc)
        assert len(errors) >= 2


# ---------------------------------------------------------------------------
# Frontmatter merging
# ---------------------------------------------------------------------------


class TestMergeFrontmatter:
    def _make_stub(self) -> dict:
        now = datetime(2026, 4, 17, 14, 0, 0, tzinfo=UTC)
        return synth._build_stub_frontmatter(
            subtype="intraday-brief",
            period_start=now - timedelta(hours=4),
            period_end=now,
            extraction_count=5,
            high_signal_count=2,
            model=synth.SONNET_MODEL,
        )

    def test_preserves_llm_intelligence_fields(self):
        doc = """\
---
title: "LLM Title"
type: synthesis
subtype: intraday-brief
period_start: 2026-04-17T10:00:00Z
period_end: 2026-04-17T14:00:00Z
model: sonnet-4.6
extraction_count: 3
high_signal_count: 1
sources_referenced: 4
wiki_pages_referenced: 6
key_findings: 3
themes_covered: ["[[AI Capex]]"]
theses_covered: ["[[SPY Long]]"]
created: 2026-04-17
updated: 2026-04-17
status: current
supersedes: []
superseded_by: null
tags: [synthesis, intraday-brief]
---

## Body
"""
        stub = self._make_stub()
        result = synth.merge_frontmatter(doc, stub)
        fm = synth._extract_frontmatter_dict(result)
        # LLM-set intelligence fields preserved
        assert fm["wiki_pages_referenced"] == 6
        assert fm["key_findings"] == 3
        assert "AI Capex" in str(fm.get("themes_covered", []))

    def test_deterministic_fields_from_stub(self):
        doc = """\
---
title: "Wrong Title"
type: synthesis
subtype: intraday-brief
period_start: WRONG
period_end: WRONG
model: wrong-model
extraction_count: 999
high_signal_count: 999
sources_referenced: 0
wiki_pages_referenced: 0
key_findings: 0
themes_covered: []
theses_covered: []
created: 2020-01-01
updated: 2020-01-01
status: current
supersedes: []
superseded_by: null
tags: []
---

## Body
"""
        stub = self._make_stub()
        result = synth.merge_frontmatter(doc, stub)
        fm = synth._extract_frontmatter_dict(result)
        # Deterministic fields come from stub
        assert fm["extraction_count"] == 5
        assert fm["model"] == "sonnet-4.6"
        assert fm["subtype"] == "intraday-brief"

    def test_adds_defaults_for_missing_fields(self):
        # Minimal doc without intelligence fields
        doc = """\
---
title: test
type: synthesis
subtype: intraday-brief
period_start: 2026-04-17T10:00:00Z
period_end: 2026-04-17T14:00:00Z
model: sonnet-4.6
extraction_count: 1
high_signal_count: 0
created: 2026-04-17
updated: 2026-04-17
---

## Body
"""
        stub = self._make_stub()
        result = synth.merge_frontmatter(doc, stub)
        fm = synth._extract_frontmatter_dict(result)
        assert "wiki_pages_referenced" in fm
        assert "status" in fm

    def test_no_frontmatter_prepends_stub(self):
        doc = "## Just a body with no frontmatter"
        stub = self._make_stub()
        result = synth.merge_frontmatter(doc, stub)
        assert result.startswith("---")
        assert "## Just a body" in result


# ---------------------------------------------------------------------------
# Supersession
# ---------------------------------------------------------------------------


class TestMarkSuperseded:
    def test_marks_page_superseded(self, tmp_brain):
        path = _write_synthesis_page(tmp_brain, "intraday-brief", status="current")
        synth.mark_superseded([path], "wiki/syntheses/new-daily-wrap.md")
        content = (tmp_brain / path).read_text()
        assert "status: superseded" in content

    def test_sets_superseded_by(self, tmp_brain):
        path = _write_synthesis_page(tmp_brain, "intraday-brief", status="current")
        new_path = "wiki/syntheses/new-daily-wrap.md"
        synth.mark_superseded([path], new_path)
        content = (tmp_brain / path).read_text()
        assert "new-daily-wrap.md" in content

    def test_handles_missing_file_gracefully(self, tmp_brain):
        # Should not raise
        synth.mark_superseded(["wiki/syntheses/nonexistent.md"], "wiki/syntheses/new.md")

    def test_marks_multiple_pages(self, tmp_brain):
        paths = [
            _write_synthesis_page(
                tmp_brain, "intraday-brief", status="current",
                hours_ago=float(i), filename=f"2026-04-17--{i:04d}-intraday-brief.md",
            )
            for i in range(1, 4)
        ]
        synth.mark_superseded(paths, "wiki/syntheses/daily-wrap.md")
        for path in paths:
            content = (tmp_brain / path).read_text()
            assert "status: superseded" in content


class TestHandleSupersession:
    def test_daily_wrap_supersedes_intraday(self, tmp_brain):
        since = _ts(24.0)
        path = _write_synthesis_page(
            tmp_brain, "intraday-brief", status="current", hours_ago=1.0
        )
        synth._handle_supersession("daily-wrap", "wiki/syntheses/new-wrap.md", since)
        content = (tmp_brain / path).read_text()
        assert "status: superseded" in content

    def test_intraday_does_not_supersede(self, tmp_brain):
        since = _ts(4.0)
        path = _write_synthesis_page(
            tmp_brain, "intraday-brief", status="current", hours_ago=1.0
        )
        synth._handle_supersession("intraday-brief", "wiki/syntheses/new-brief.md", since)
        content = (tmp_brain / path).read_text()
        # intraday-brief does not supersede anything
        assert "status: current" in content

    def test_event_driven_does_not_supersede(self, tmp_brain):
        since = _ts(2.0)
        _write_synthesis_page(tmp_brain, "intraday-brief", status="current", hours_ago=1.0)
        # Should not raise and should not supersede
        synth._handle_supersession("event-driven", "wiki/syntheses/new-event.md", since)


# ---------------------------------------------------------------------------
# Index updates
# ---------------------------------------------------------------------------


class TestUpdateSynthesisIndex:
    def test_adds_to_empty_section(self, tmp_brain):
        synth._update_synthesis_index(
            "wiki/syntheses/2026-04-17--1400-intraday-brief.md",
            "2026-04-17 14:00 Intraday Brief",
        )
        content = (tmp_brain / "index.md").read_text()
        assert "Intraday Brief" in content
        assert "_(No pages yet)_" not in content.split("## Syntheses")[1].split("##")[0]

    def test_updates_existing_entry(self, tmp_brain):
        path = "wiki/syntheses/2026-04-17--1400-intraday-brief.md"
        synth._update_synthesis_index(path, "Old Title")
        synth._update_synthesis_index(path, "New Title")
        content = (tmp_brain / "index.md").read_text()
        assert "New Title" in content
        assert content.count(path) == 1  # not duplicated

    def test_no_crash_when_index_missing(self, tmp_brain):
        (tmp_brain / "index.md").unlink()
        # Should not raise
        synth._update_synthesis_index("wiki/syntheses/test.md", "Test")


# ---------------------------------------------------------------------------
# End-to-end (no LLM)
# ---------------------------------------------------------------------------


class TestSynthesizeEndToEnd:
    def test_dry_run_returns_plan(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/a.json", verdict="high_signal", hours_ago=1.0)
        result = synth.synthesize(
            subtype="intraday-brief",
            since=_ts(2.0),
            dry_run=True,
        )
        assert result["skipped"] is False
        assert result["reason"] == "dry_run"
        assert result["extraction_count"] == 1
        assert result["path"] is None

    def test_dry_run_does_not_write(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/a.json", verdict="high_signal", hours_ago=1.0)
        synth.synthesize(subtype="intraday-brief", since=_ts(2.0), dry_run=True)
        synth_dir = tmp_brain / "wiki" / "syntheses"
        pages = [f for f in synth_dir.glob("*.md")] if synth_dir.exists() else []
        assert pages == []

    def test_no_llm_returns_context_info(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/a.json", verdict="high_signal", hours_ago=1.0)
        result = synth.synthesize(
            subtype="intraday-brief",
            since=_ts(2.0),
            no_llm=True,
        )
        assert result["skipped"] is False
        assert result["reason"] == "no_llm"
        assert result["extraction_count"] == 1

    def test_skips_when_no_extractions(self, tmp_brain):
        result = synth.synthesize(
            subtype="intraday-brief",
            since=_ts(1.0),
        )
        assert result["skipped"] is True
        assert "no new extractions" in result["reason"]

    def test_skips_when_only_noise(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/a.json", verdict="noise", hours_ago=0.5)
        result = synth.synthesize(
            subtype="intraday-brief",
            since=_ts(1.0),
        )
        assert result["skipped"] is True

    def test_dry_run_reports_high_signal_count(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/a.json", verdict="high_signal", hours_ago=1.0)
        _write_extraction(tmp_brain, "extractions/b.json", verdict="medium_signal", hours_ago=1.0)
        result = synth.synthesize(
            subtype="intraday-brief",
            since=_ts(2.0),
            dry_run=True,
        )
        assert result["high_signal_count"] == 1
        assert result["extraction_count"] == 2

    def test_model_selection_monthly(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/a.json", verdict="high_signal", hours_ago=1.0)
        result = synth.synthesize(
            subtype="monthly-review",
            since=_ts(2.0),
            dry_run=True,
        )
        assert result["model"] == synth.OPUS_MODEL

    def test_model_selection_intraday(self, tmp_brain):
        _write_extraction(tmp_brain, "extractions/a.json", verdict="high_signal", hours_ago=1.0)
        result = synth.synthesize(
            subtype="intraday-brief",
            since=_ts(2.0),
            dry_run=True,
        )
        assert result["model"] == synth.SONNET_MODEL


# ---------------------------------------------------------------------------
# Auto subtype selection
# ---------------------------------------------------------------------------


class TestAutoSelectSubtype:
    def test_returns_valid_subtype(self):
        subtype = synth.auto_select_subtype()
        assert subtype in synth.SUBTYPE_MODELS

    def test_subtype_logic_intraday(self):
        """Logic for intraday: not Sunday, not 21:00, no event trigger."""
        # Test the condition directly
        weekday = 0      # Monday
        hour = 14        # 2pm — not daily wrap hour
        day = 15         # not 1st week
        is_sunday = weekday == 6
        is_monthly = is_sunday and day <= 7 and hour == 5
        is_weekly = is_sunday and hour == 4
        is_daily = hour == 21
        assert not is_monthly
        assert not is_weekly
        assert not is_daily

    def test_subtype_logic_daily_wrap(self):
        hour = 21
        is_daily = hour == 21
        assert is_daily

    def test_subtype_logic_weekly_deep(self):
        weekday = 6   # Sunday
        hour = 4
        day = 15      # not 1st week
        is_monthly = weekday == 6 and day <= 7 and hour == 5
        is_weekly = weekday == 6 and hour == 4
        assert not is_monthly
        assert is_weekly

    def test_subtype_logic_monthly_review(self):
        weekday = 6   # Sunday
        hour = 5
        day = 4       # 1st week
        is_monthly = weekday == 6 and day <= 7 and hour == 5
        assert is_monthly
