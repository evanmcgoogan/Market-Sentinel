"""
serve.py — Scheduler + health endpoint for the Signal Hunter Digital Brain.

Runs the full ingestion → extraction → compilation → synthesis pipeline on a
market-aware schedule. Three modes (Active, Watch, Sleep) adjust polling
intervals based on time of day and market hours.

Pipeline:
    ingest (Twitter, YouTube, markets, arXiv, newsletters)
    → extract (Haiku: raw → structured JSON)
    → compile (Sonnet: extractions → wiki pages)
    → synthesize (Sonnet/Opus: wiki + extractions → intelligence output)

Synthesis schedule (ET):
    intraday-brief  — 10:00, 14:00, 18:00, 20:00 daily
    daily-wrap      — 21:00 daily
    weekly-deep     — Sunday 04:00
    monthly-review  — 1st Sunday 05:00 (Opus)
    event-driven    — triggered after compilation when 3+ high_signal in 2h

Usage:
    python scripts/serve.py                  # Start scheduler + health server
    python scripts/serve.py --port 8080      # Custom port
    python scripts/serve.py --once           # Run one full cycle and exit

Environment variables:
    SOCIALDATA_API_KEY  — Required for Twitter ingestion
    ANTHROPIC_API_KEY   — Required for extraction (Haiku), compilation (Sonnet),
                          and synthesis (Sonnet/Opus)
    PORT                — Server port (default 10000, Railway sets this)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
import uvicorn

# Add scripts/ to path for sibling imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

from brain_io import append_log, brain_root, utcnow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schedule configuration
# ---------------------------------------------------------------------------

def load_schedule_config() -> dict[str, Any]:
    """Load config/polling-schedule.yaml."""
    path = brain_root() / "config" / "polling-schedule.yaml"
    with open(path) as f:
        return yaml.safe_load(f) or {}


def current_mode() -> str:
    """Determine which polling mode to use based on current ET time.

    Returns: "active" | "watch" | "sleep"
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    now = datetime.now(ZoneInfo("America/New_York"))
    weekday = now.weekday()  # 0=Mon, 6=Sun
    hour, minute = now.hour, now.minute
    time_val = hour * 60 + minute  # minutes since midnight

    # Active: Mon-Fri 09:00-16:30 ET
    if 0 <= weekday <= 4 and 540 <= time_val <= 990:
        return "active"

    # Watch: Mon-Fri 16:30-20:00 ET, Sun 18:00-23:59 ET
    if 0 <= weekday <= 4 and 990 < time_val <= 1200:
        return "watch"
    if weekday == 6 and time_val >= 1080:  # Sunday 18:00+
        return "watch"

    return "sleep"


def get_intervals(mode: str, config: dict[str, Any]) -> dict[str, int]:
    """Get polling intervals in minutes for the given mode."""
    mode_cfg = config.get("modes", {}).get(mode, {})
    return {
        "twitter": mode_cfg.get("twitter_poll_minutes", 30),
        "youtube": mode_cfg.get("youtube_check_minutes", 60),
        "markets": mode_cfg.get("market_poll_minutes", 60),
        "arxiv": mode_cfg.get("arxiv_poll_minutes", 240),
        "substack": mode_cfg.get("substack_poll_minutes", 240),
    }


# ---------------------------------------------------------------------------
# Pipeline steps — thin wrappers around the existing scripts
# ---------------------------------------------------------------------------

_last_run: dict[str, str] = {}
_run_counts: dict[str, int] = {}
_errors: dict[str, str] = {}


def _record_run(step: str, success: bool, error: str = "") -> None:
    """Record a pipeline step execution for health reporting."""
    _last_run[step] = utcnow().isoformat()
    _run_counts[step] = _run_counts.get(step, 0) + 1
    if not success:
        _errors[step] = error
    elif step in _errors:
        del _errors[step]


async def run_twitter_ingestion() -> None:
    """Run Twitter ingestion for all configured accounts."""
    if not os.environ.get("SOCIALDATA_API_KEY"):
        logger.warning("SOCIALDATA_API_KEY not set — skipping Twitter ingestion")
        return

    logger.info("Running Twitter ingestion...")
    try:
        from ingest_twitter import ingest_all
        result = await ingest_all()
        _record_run("twitter", True)
        logger.info("Twitter ingestion complete: %s", result)
    except Exception as e:
        _record_run("twitter", False, str(e))
        logger.error("Twitter ingestion failed: %s", e)


async def run_youtube_ingestion() -> None:
    """Run YouTube ingestion for all configured channels."""
    logger.info("Running YouTube ingestion...")
    try:
        from ingest_youtube import ingest_all
        result = await ingest_all()
        _record_run("youtube", True)
        logger.info("YouTube ingestion complete: %s", result)
    except Exception as e:
        _record_run("youtube", False, str(e))
        logger.error("YouTube ingestion failed: %s", e)


async def run_market_ingestion() -> None:
    """Run market data ingestion (Polymarket, Kalshi, price feeds)."""
    logger.info("Running market ingestion...")
    try:
        from ingest_markets import ingest_all
        result = await ingest_all()
        _record_run("markets", True)
        logger.info("Market ingestion complete: %s", result)
    except Exception as e:
        _record_run("markets", False, str(e))
        logger.error("Market ingestion failed: %s", e)


async def run_arxiv_ingestion() -> None:
    """Run arXiv paper ingestion for configured categories and author watchlist."""
    logger.info("Running arXiv ingestion...")
    try:
        from ingest_arxiv import ingest_all
        result = await ingest_all()
        _record_run("arxiv", True)
        logger.info("arXiv ingestion complete: %s", result)
    except Exception as e:
        _record_run("arxiv", False, str(e))
        logger.error("arXiv ingestion failed: %s", e)


async def run_substack_ingestion() -> None:
    """Run newsletter ingestion for configured Substack/RSS feeds."""
    logger.info("Running newsletter ingestion...")
    try:
        from ingest_substack import ingest_all
        result = await ingest_all()
        _record_run("substack", True)
        logger.info("Newsletter ingestion complete: %s", result)
    except Exception as e:
        _record_run("substack", False, str(e))
        logger.error("Newsletter ingestion failed: %s", e)


async def run_extraction() -> None:
    """Run Haiku extraction on all unextracted raw files."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.warning("ANTHROPIC_API_KEY not set — skipping extraction")
        return

    logger.info("Running extraction pipeline...")
    try:
        from extract import extract_all
        result = await extract_all()
        _record_run("extraction", True)
        logger.info("Extraction complete: %s", result)
    except Exception as e:
        _record_run("extraction", False, str(e))
        logger.error("Extraction failed: %s", e)


async def run_compilation() -> None:
    """Run Sonnet compilation on all uncompiled extractions."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.warning("ANTHROPIC_API_KEY not set — skipping compilation")
        return

    logger.info("Running compilation pipeline...")
    try:
        from compile import compile_all
        result = await compile_all()
        _record_run("compilation", True)
        logger.info("Compilation complete: %s", result)
    except Exception as e:
        _record_run("compilation", False, str(e))
        logger.error("Compilation failed: %s", e)


async def run_synthesis(subtype: str = "intraday-brief") -> None:
    """Run synthesis of the given subtype.

    Calls synthesize.synthesize() in a thread so the sync LLM call doesn't
    block the event loop. The synthesize module handles skip conditions
    (no new extractions, all noise) — so calling this frequently is safe.
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.warning("ANTHROPIC_API_KEY not set — skipping synthesis")
        return

    logger.info("Running %s synthesis...", subtype)
    try:
        from synthesize import synthesize as _synthesize
        result = await asyncio.to_thread(lambda: _synthesize(subtype=subtype))
        step_key = f"synthesis_{subtype.replace('-', '_')}"
        if result.get("skipped"):
            logger.info("Synthesis skipped (%s): %s", subtype, result.get("reason"))
        else:
            logger.info(
                "Synthesis complete (%s) | extractions: %d | high_signal: %d | path: %s",
                subtype,
                result.get("extraction_count", 0),
                result.get("high_signal_count", 0),
                result.get("path"),
            )
        _record_run(step_key, True)
    except Exception as e:
        step_key = f"synthesis_{subtype.replace('-', '_')}"
        _record_run(step_key, False, str(e))
        logger.error("Synthesis failed (%s): %s", subtype, e)


async def run_event_driven_synthesis() -> None:
    """Check for a high-signal burst and trigger event-driven synthesis if warranted.

    Called after each compilation run. The trigger threshold is 3+ high_signal
    extractions within a 2-hour window (defined in synthesize.py).
    Only fires if ANTHROPIC_API_KEY is available.
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return
    try:
        from synthesize import is_event_driven_trigger
        if is_event_driven_trigger():
            logger.info("High-signal burst detected — triggering event-driven synthesis")
            await run_synthesis("event-driven")
    except Exception as e:
        logger.error("Event-driven synthesis check failed: %s", e)


async def run_git_commit() -> None:
    """Auto-commit changes if there are any."""
    try:
        root = str(brain_root())
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True, text=True, cwd=root,
        )
        if not status.stdout.strip():
            logger.debug("No changes to commit")
            return

        subprocess.run(
            ["git", "add", "-A"],
            capture_output=True, text=True, cwd=root,
        )
        timestamp = utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        subprocess.run(
            ["git", "commit", "-m", f"Auto-commit: brain update {timestamp}"],
            capture_output=True, text=True, cwd=root,
        )
        _record_run("git_commit", True)
        logger.info("Auto-committed changes at %s", timestamp)
    except Exception as e:
        _record_run("git_commit", False, str(e))
        logger.error("Git commit failed: %s", e)


async def run_full_cycle() -> dict[str, Any]:
    """Run one complete ingestion → extraction → compilation → synthesis cycle.

    Steps run sequentially so each stage has the latest output from the previous.
    Synthesis only fires if there's a high-signal burst (event-driven trigger);
    scheduled synthesis runs on separate cron jobs in the scheduler.
    """
    cycle_start = utcnow()
    mode = current_mode()
    logger.info("Starting full cycle in %s mode", mode)
    append_log(f"CYCLE START | mode: {mode}")

    # Step 1: Ingest from all sources (parallel — they're independent)
    await asyncio.gather(
        run_twitter_ingestion(),
        run_youtube_ingestion(),
        run_market_ingestion(),
        run_arxiv_ingestion(),
        run_substack_ingestion(),
    )

    # Step 2: Extract signals from new raw files
    await run_extraction()

    # Step 3: Compile extractions into wiki pages
    await run_compilation()

    # Step 4: Check for event-driven synthesis trigger (burst of high-signal extractions)
    await run_event_driven_synthesis()

    elapsed = (utcnow() - cycle_start).total_seconds()
    append_log(f"CYCLE COMPLETE | mode: {mode} | elapsed: {elapsed:.1f}s")
    logger.info("Full cycle complete in %.1fs", elapsed)

    return {
        "mode": mode,
        "elapsed_seconds": round(elapsed, 1),
        "last_run": dict(_last_run),
        "run_counts": dict(_run_counts),
        "errors": dict(_errors),
    }


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def build_scheduler(config: dict[str, Any]) -> AsyncIOScheduler:
    """Create and configure the APScheduler instance.

    Uses market-aware intervals: polls frequently during active hours,
    less during watch mode, minimally during sleep.
    """
    scheduler = AsyncIOScheduler(timezone="America/New_York")

    # The "adaptive" approach: run a full cycle periodically.
    # During active mode, the cycle runs every 10 minutes.
    # The cycle itself checks current_mode() and adjusts behavior.
    # Individual source intervals within each cycle are handled by
    # the ingestion scripts' internal dedup (same content won't be re-ingested).

    # Full pipeline cycle — runs frequently, dedup prevents redundant work
    scheduler.add_job(
        run_full_cycle,
        IntervalTrigger(minutes=10),
        id="full_cycle",
        name="Full ingestion→extraction→compilation cycle",
        max_instances=1,  # Don't overlap cycles
        misfire_grace_time=300,
    )

    # Git auto-commit — every 60 minutes
    scheduler.add_job(
        run_git_commit,
        IntervalTrigger(minutes=60),
        id="git_commit",
        name="Auto-commit wiki changes",
        max_instances=1,
    )

    # -----------------------------------------------------------------------
    # Synthesis schedule (all times ET)
    # -----------------------------------------------------------------------

    # Intraday brief — 4x per day: 10:00, 14:00, 18:00, 20:00
    # synthesize.py skip conditions prevent no-op LLM calls when nothing is new
    scheduler.add_job(
        run_synthesis,
        CronTrigger(hour="10,14,18,20", minute=0, timezone="America/New_York"),
        kwargs={"subtype": "intraday-brief"},
        id="synthesis_intraday",
        name="Intraday brief synthesis (Sonnet)",
        max_instances=1,
        misfire_grace_time=600,
    )

    # Daily wrap — 21:00 ET every day
    scheduler.add_job(
        run_synthesis,
        CronTrigger(hour=21, minute=0, timezone="America/New_York"),
        kwargs={"subtype": "daily-wrap"},
        id="synthesis_daily",
        name="Daily wrap synthesis (Sonnet)",
        max_instances=1,
        misfire_grace_time=600,
    )

    # Weekly deep — every Sunday at 04:00 ET
    scheduler.add_job(
        run_synthesis,
        CronTrigger(day_of_week="sun", hour=4, minute=0, timezone="America/New_York"),
        kwargs={"subtype": "weekly-deep"},
        id="synthesis_weekly",
        name="Weekly deep synthesis (Sonnet)",
        max_instances=1,
        misfire_grace_time=600,
    )

    # Monthly review — 1st Sunday of month at 05:00 ET (Opus)
    scheduler.add_job(
        run_synthesis,
        CronTrigger(
            day_of_week="sun", day="1-7", hour=5, minute=0,
            timezone="America/New_York",
        ),
        kwargs={"subtype": "monthly-review"},
        id="synthesis_monthly",
        name="Monthly review synthesis (Opus)",
        max_instances=1,
        misfire_grace_time=600,
    )

    return scheduler


# ---------------------------------------------------------------------------
# FastAPI health endpoint
# ---------------------------------------------------------------------------

app = FastAPI(title="Signal Hunter Brain", version="3.0.0")


@app.get("/health")
async def health() -> dict[str, Any]:
    """Health check endpoint for Render and monitoring."""
    mode = current_mode()
    return {
        "status": "healthy" if not _errors else "degraded",
        "mode": mode,
        "version": "3.0.0",
        "uptime_runs": dict(_run_counts),
        "last_run": dict(_last_run),
        "errors": dict(_errors) if _errors else None,
        "timestamp": utcnow().isoformat(),
    }


@app.get("/status")
async def status() -> dict[str, Any]:
    """Detailed status including source counts, pipeline state, and synthesis state."""
    from brain_io import load_sources_config

    cfg = load_sources_config()
    twitter_accounts = len(cfg.get("twitter", {}).get("accounts", []))
    youtube_channels = len(cfg.get("youtube", {}).get("channels", []))

    # Count raw files, extractions, wiki pages
    raw_count = sum(1 for _ in (brain_root() / "raw").rglob("*.md")) if (brain_root() / "raw").exists() else 0
    ext_count = sum(1 for _ in (brain_root() / "extractions").rglob("*.json")) if (brain_root() / "extractions").exists() else 0
    wiki_count = sum(1 for _ in (brain_root() / "wiki").rglob("*.md")) if (brain_root() / "wiki").exists() else 0

    # Load synthesis state
    synthesis_state: dict[str, Any] = {}
    try:
        from synthesize import load_synthesis_state
        synthesis_state = load_synthesis_state()
    except Exception:
        pass

    return {
        "sources": {
            "twitter_accounts": twitter_accounts,
            "youtube_channels": youtube_channels,
            "has_socialdata_key": bool(os.environ.get("SOCIALDATA_API_KEY")),
            "has_anthropic_key": bool(os.environ.get("ANTHROPIC_API_KEY")),
        },
        "pipeline": {
            "raw_files": raw_count,
            "extractions": ext_count,
            "wiki_pages": wiki_count,
        },
        "synthesis": {
            subtype: {
                "last_run": entry.get("last_run"),
                "last_path": entry.get("last_path"),
            }
            for subtype, entry in synthesis_state.items()
        },
        "scheduler": {
            "mode": current_mode(),
            "run_counts": dict(_run_counts),
            "last_run": dict(_last_run),
            "errors": dict(_errors) if _errors else None,
        },
    }


@app.post("/trigger")
async def trigger_cycle() -> dict[str, Any]:
    """Manually trigger a full pipeline cycle."""
    result = await run_full_cycle()
    return {"triggered": True, "result": result}


@app.post("/trigger/synthesis/{subtype}")
async def trigger_synthesis(subtype: str) -> dict[str, Any]:
    """Manually trigger a synthesis run for the given subtype.

    Valid subtypes: intraday-brief, daily-wrap, weekly-deep, monthly-review, event-driven
    """
    valid = {"intraday-brief", "daily-wrap", "weekly-deep", "monthly-review", "event-driven"}
    if subtype not in valid:
        return {"error": f"unknown subtype: {subtype!r}. Valid: {sorted(valid)}"}
    await run_synthesis(subtype)
    return {"triggered": True, "subtype": subtype}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Signal Hunter Brain — scheduler + health endpoint")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "10000")))
    parser.add_argument("--once", action="store_true", help="Run one full cycle and exit")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.once:
        logger.info("Running single cycle...")
        result = asyncio.run(run_full_cycle())
        logger.info("Cycle result: %s", result)
        return

    # Start scheduler
    config = load_schedule_config()
    scheduler = build_scheduler(config)
    scheduler.start()

    append_log(f"SERVE STARTED | port: {args.port} | mode: {current_mode()}")
    logger.info("Brain server starting on port %d in %s mode", args.port, current_mode())

    # Run FastAPI with uvicorn
    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="info")


if __name__ == "__main__":
    main()
