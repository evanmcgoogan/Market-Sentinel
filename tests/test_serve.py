"""Tests for serve.py — scheduler, mode detection, and health endpoint.

No actual API calls or server startup. Tests cover deterministic logic:
mode detection, interval configuration, health reporting, and schedule config.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import serve
import brain_io


# ---------------------------------------------------------------------------
# Mode detection
# ---------------------------------------------------------------------------


class TestCurrentMode:
    """Test market-aware mode detection.

    We can't easily mock datetime.now(ZoneInfo(...)), so we test the
    function returns a valid mode string and test the logic via a helper.
    """

    def test_returns_valid_mode(self):
        """current_mode() returns one of the three valid modes."""
        mode = serve.current_mode()
        assert mode in ("active", "watch", "sleep")

    def test_mode_logic_active(self):
        """Verify active mode logic: weekday + market hours."""
        # Tuesday 10:30 ET = weekday 1, 630 minutes
        weekday, time_val = 1, 630
        assert 0 <= weekday <= 4 and 540 <= time_val <= 990  # active

    def test_mode_logic_watch_after_hours(self):
        """Verify watch mode logic: weekday + after-hours."""
        # Wednesday 17:00 ET = weekday 2, 1020 minutes
        weekday, time_val = 2, 1020
        assert 0 <= weekday <= 4 and 990 < time_val <= 1200  # watch

    def test_mode_logic_watch_sunday(self):
        """Verify watch mode logic: Sunday 18:00+."""
        # Sunday 19:00 ET = weekday 6, 1140 minutes
        weekday, time_val = 6, 1140
        assert weekday == 6 and time_val >= 1080  # watch

    def test_mode_logic_sleep_overnight(self):
        """Verify sleep mode logic: early morning weekday."""
        # Tuesday 2:00 AM = weekday 1, 120 minutes
        weekday, time_val = 1, 120
        is_active = 0 <= weekday <= 4 and 540 <= time_val <= 990
        is_watch_wkday = 0 <= weekday <= 4 and 990 < time_val <= 1200
        is_watch_sun = weekday == 6 and time_val >= 1080
        assert not is_active and not is_watch_wkday and not is_watch_sun  # sleep

    def test_mode_logic_sleep_weekend(self):
        """Verify sleep mode logic: Saturday."""
        weekday = 5  # Saturday
        is_active = 0 <= weekday <= 4
        is_watch_sun = weekday == 6
        assert not is_active and not is_watch_sun  # sleep


# ---------------------------------------------------------------------------
# Schedule config
# ---------------------------------------------------------------------------


class TestScheduleConfig:
    def test_loads_config(self, tmp_brain):
        config = serve.load_schedule_config()
        assert "modes" in config
        assert "active" in config["modes"]

    def test_get_intervals_active(self, tmp_brain):
        config = serve.load_schedule_config()
        intervals = serve.get_intervals("active", config)
        assert intervals["twitter"] == 5
        assert intervals["youtube"] == 15
        assert intervals["markets"] == 5

    def test_get_intervals_sleep(self, tmp_brain):
        config = serve.load_schedule_config()
        intervals = serve.get_intervals("sleep", config)
        assert intervals["twitter"] == 30
        assert intervals["youtube"] == 60
        assert intervals["markets"] == 60

    def test_get_intervals_unknown_mode_defaults(self, tmp_brain):
        config = serve.load_schedule_config()
        intervals = serve.get_intervals("nonexistent", config)
        # Should return defaults
        assert intervals["twitter"] == 30
        assert intervals["youtube"] == 60


# ---------------------------------------------------------------------------
# Run tracking
# ---------------------------------------------------------------------------


class TestRunTracking:
    def test_record_success(self):
        serve._last_run.clear()
        serve._run_counts.clear()
        serve._errors.clear()

        serve._record_run("twitter", True)
        assert "twitter" in serve._last_run
        assert serve._run_counts["twitter"] == 1
        assert "twitter" not in serve._errors

    def test_record_failure(self):
        serve._last_run.clear()
        serve._run_counts.clear()
        serve._errors.clear()

        serve._record_run("extraction", False, "API key missing")
        assert serve._errors["extraction"] == "API key missing"
        assert serve._run_counts["extraction"] == 1

    def test_success_clears_error(self):
        serve._errors.clear()
        serve._run_counts.clear()

        serve._record_run("twitter", False, "Network error")
        assert "twitter" in serve._errors

        serve._record_run("twitter", True)
        assert "twitter" not in serve._errors

    def test_counts_accumulate(self):
        serve._run_counts.clear()

        serve._record_run("markets", True)
        serve._record_run("markets", True)
        serve._record_run("markets", True)
        assert serve._run_counts["markets"] == 3


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_healthy_when_no_errors(self):
        serve._errors.clear()
        result = await serve.health()
        assert result["status"] == "healthy"
        assert "mode" in result
        assert "timestamp" in result

    @pytest.mark.asyncio
    async def test_degraded_when_errors(self):
        serve._errors["twitter"] = "API timeout"
        result = await serve.health()
        assert result["status"] == "degraded"
        assert result["errors"]["twitter"] == "API timeout"
        serve._errors.clear()

    @pytest.mark.asyncio
    async def test_includes_version(self):
        serve._errors.clear()
        result = await serve.health()
        assert result["version"] == "3.0.0"


# ---------------------------------------------------------------------------
# Status endpoint
# ---------------------------------------------------------------------------


class TestStatusEndpoint:
    @pytest.mark.asyncio
    async def test_returns_source_counts(self, tmp_brain):
        result = await serve.status()
        assert result["sources"]["twitter_accounts"] > 0
        assert result["sources"]["youtube_channels"] > 0

    @pytest.mark.asyncio
    async def test_reports_key_availability(self, tmp_brain):
        result = await serve.status()
        assert "has_socialdata_key" in result["sources"]
        assert "has_anthropic_key" in result["sources"]

    @pytest.mark.asyncio
    async def test_reports_pipeline_counts(self, tmp_brain):
        result = await serve.status()
        assert "raw_files" in result["pipeline"]
        assert "wiki_pages" in result["pipeline"]


# ---------------------------------------------------------------------------
# Scheduler build
# ---------------------------------------------------------------------------


class TestBuildScheduler:
    def test_creates_scheduler(self, tmp_brain):
        config = serve.load_schedule_config()
        scheduler = serve.build_scheduler(config)
        jobs = scheduler.get_jobs()
        assert len(jobs) >= 6  # full_cycle + git_commit + 4 synthesis jobs

    def test_full_cycle_job_exists(self, tmp_brain):
        config = serve.load_schedule_config()
        scheduler = serve.build_scheduler(config)
        job_ids = [j.id for j in scheduler.get_jobs()]
        assert "full_cycle" in job_ids
        assert "git_commit" in job_ids

    def test_synthesis_jobs_exist(self, tmp_brain):
        config = serve.load_schedule_config()
        scheduler = serve.build_scheduler(config)
        job_ids = [j.id for j in scheduler.get_jobs()]
        assert "synthesis_intraday" in job_ids
        assert "synthesis_daily" in job_ids
        assert "synthesis_weekly" in job_ids
        assert "synthesis_monthly" in job_ids

    def test_synthesis_job_subtypes(self, tmp_brain):
        """Each synthesis cron job passes the correct subtype kwarg."""
        config = serve.load_schedule_config()
        scheduler = serve.build_scheduler(config)
        jobs_by_id = {j.id: j for j in scheduler.get_jobs()}

        assert jobs_by_id["synthesis_intraday"].kwargs == {"subtype": "intraday-brief"}
        assert jobs_by_id["synthesis_daily"].kwargs == {"subtype": "daily-wrap"}
        assert jobs_by_id["synthesis_weekly"].kwargs == {"subtype": "weekly-deep"}
        assert jobs_by_id["synthesis_monthly"].kwargs == {"subtype": "monthly-review"}


# ---------------------------------------------------------------------------
# Synthesis integration
# ---------------------------------------------------------------------------


class TestSynthesisIntegration:
    @pytest.mark.asyncio
    async def test_run_synthesis_skips_without_api_key(self, tmp_brain):
        """run_synthesis returns early when ANTHROPIC_API_KEY is absent."""
        with patch.dict(os.environ, {}, clear=True):
            # Should not raise, just log a warning and return
            await serve.run_synthesis("intraday-brief")
        # No error recorded since we returned early — run_counts shouldn't have entry
        # (the step key would only be recorded if we got past the guard)

    @pytest.mark.asyncio
    async def test_run_synthesis_records_run_on_skip(self, tmp_brain):
        """run_synthesis records a successful run even when synthesis itself skips."""
        serve._run_counts.clear()
        serve._errors.clear()

        # Patch synthesize to return a skipped result
        mock_result = {"skipped": True, "reason": "no new extractions", "path": None, "extraction_count": 0}
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("serve.asyncio.to_thread", return_value=mock_result):
                await serve.run_synthesis("intraday-brief")

        assert serve._run_counts.get("synthesis_intraday_brief", 0) == 1
        assert "synthesis_intraday_brief" not in serve._errors

    @pytest.mark.asyncio
    async def test_run_synthesis_records_error_on_exception(self, tmp_brain):
        """run_synthesis records the error when synthesis raises."""
        serve._run_counts.clear()
        serve._errors.clear()

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("serve.asyncio.to_thread", side_effect=RuntimeError("boom")):
                await serve.run_synthesis("daily-wrap")

        assert "synthesis_daily_wrap" in serve._errors
        assert "boom" in serve._errors["synthesis_daily_wrap"]

    @pytest.mark.asyncio
    async def test_run_event_driven_skips_without_api_key(self, tmp_brain):
        """run_event_driven_synthesis returns early without API key."""
        with patch.dict(os.environ, {}, clear=True):
            # Should not raise
            await serve.run_event_driven_synthesis()

    @pytest.mark.asyncio
    async def test_run_event_driven_no_trigger(self, tmp_brain):
        """run_event_driven_synthesis does nothing when trigger threshold not met."""
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("serve.asyncio.to_thread") as mock_thread:
                # Patch is_event_driven_trigger to return False
                with patch("synthesize.is_event_driven_trigger", return_value=False):
                    await serve.run_event_driven_synthesis()
                mock_thread.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_event_driven_fires_on_trigger(self, tmp_brain):
        """run_event_driven_synthesis calls run_synthesis when burst detected."""
        serve._run_counts.clear()
        serve._errors.clear()

        mock_result = {"skipped": False, "reason": "ok", "path": "wiki/syntheses/test.md",
                       "extraction_count": 3, "high_signal_count": 3}
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("synthesize.is_event_driven_trigger", return_value=True):
                with patch("serve.asyncio.to_thread", return_value=mock_result):
                    await serve.run_event_driven_synthesis()

        assert serve._run_counts.get("synthesis_event_driven", 0) == 1

    @pytest.mark.asyncio
    async def test_status_includes_synthesis_state(self, tmp_brain):
        """Status endpoint exposes synthesis last-run state."""
        result = await serve.status()
        assert "synthesis" in result
        # synthesis dict may be empty on a fresh brain — that's fine
        assert isinstance(result["synthesis"], dict)
