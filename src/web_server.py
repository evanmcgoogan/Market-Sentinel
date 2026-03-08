"""
Market Sentinel Dashboard — Flask web server.

Serves a real-time prediction market intelligence dashboard.
Reads from the same SQLite DB as the sentinel monitor.

Run with: python3 src/web_server.py
Then open: http://localhost:5050
"""

import os
import re
import sys
import logging
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from flask import Flask, render_template, jsonify, request

# Resolve paths relative to this file so the server can be started
# from any working directory.
SRC_DIR = Path(__file__).parent
ROOT_DIR = SRC_DIR.parent

sys.path.insert(0, str(SRC_DIR))

from config import load_config
from database import Database
from story_generator import StoryGenerator, OutlookGenerator, OutlookGrader
from whale_intelligence import WhaleBrain
from market_data import MarketDataProvider
from forecast_engine import ForecastEngine
from forecast_evaluator import ForecastEvaluator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("dashboard")

app = Flask(
    __name__,
    template_folder=str(ROOT_DIR / "dashboard"),
    static_folder=str(ROOT_DIR / "dashboard" / "static"),
    static_url_path="/static",
)

# Config resolution order:
# 1) SENTINEL_CONFIG env var (explicit override — resolved relative to ROOT_DIR)
# 2) repo-local config.json (developer/local mode)
# 3) config.example.json (safe defaults for hosted demo)
_cfg_env = os.environ.get("SENTINEL_CONFIG", "").strip()
_cfg_local = ROOT_DIR / "config.json"
_cfg_example = ROOT_DIR / "config.example.json"
if _cfg_env:
    # Resolve relative paths against ROOT_DIR (not CWD) so Render's
    # SENTINEL_CONFIG=config.example.json works regardless of working dir.
    _cfg_candidate = Path(_cfg_env)
    if not _cfg_candidate.is_absolute():
        _cfg_candidate = ROOT_DIR / _cfg_candidate
    _cfg_path = str(_cfg_candidate)
elif _cfg_local.exists():
    _cfg_path = str(_cfg_local)
elif _cfg_example.exists():
    _cfg_path = str(_cfg_example)
else:
    _cfg_path = None

config = load_config(_cfg_path)

# Resolve DB path — Render sets SENTINEL_DB_PATH for persistent disk.
_db_path_str = os.environ.get("SENTINEL_DB_PATH") or str(ROOT_DIR / config.db_path)

# Ensure the DB directory exists (Render persistent disk may not be mounted yet).
_db_dir = os.path.dirname(_db_path_str)
if _db_dir:
    os.makedirs(_db_dir, exist_ok=True)

# Try primary DB; fall back to /tmp if the persistent disk has I/O issues.
try:
    db = Database(_db_path_str)
except Exception as _db_exc:
    _fallback_db = "/tmp/market_sentinel_fallback.db"
    logger.warning(f"Primary DB at {_db_path_str} failed ({_db_exc}); falling back to {_fallback_db}")
    db = Database(_fallback_db)

# Read Anthropic key from config (or environment variable fallback)
_anthropic_key = getattr(config, "anthropic_api_key", "") or os.environ.get("ANTHROPIC_API_KEY", "")
# Pass db to StoryGenerator so ClaudeHeadlineGenerator can persist/load its
# headline cache — avoids cold-start Claude calls after a process restart.
story_gen    = StoryGenerator(api_key=_anthropic_key, db=db)
whale_brain  = WhaleBrain(api_key=_anthropic_key, db=db)
outlook_gen  = OutlookGenerator(api_key=_anthropic_key)
outlook_grader = OutlookGrader(api_key=_anthropic_key)
_claude_active = bool(_anthropic_key)

# New deterministic forecast engine + evaluator
market_data_provider = MarketDataProvider(db)
forecast_engine = ForecastEngine(market_data_provider, db, api_key=_anthropic_key)
forecast_evaluator = ForecastEvaluator(api_key=_anthropic_key)

logger.info("Claude headlines: %s", "ENABLED (haiku)" if _claude_active else "DISABLED (add anthropic_api_key to config.json)")

# Wrap DB-dependent startup in try/except — a DB issue at import time
# must NOT crash the worker, otherwise Render's health check can never pass.
try:
    db.ensure_watchlist("Default")
except Exception as exc:
    logger.warning(f"Startup: ensure_watchlist failed (non-fatal): {exc}")

# Warm in-memory caches from DB so the first request after a restart
# is served instantly rather than triggering blocking Claude/API calls.
if _claude_active:
    try:
        outlook_gen.load_from_db(db)
    except Exception as exc:
        logger.warning(f"Startup: outlook cache warmup failed (non-fatal): {exc}")
    # Clear any stale forecast cache that contains a fallback/error message
    try:
        _cached_outlook = db.get_state("api_outlook_cache", default=None)
        if _cached_outlook:
            _cached_data = _cached_outlook.get("data", {})
            _summary = _cached_data.get("outlook_summary", "")
            if "unavailable" in _summary.lower() or "not configured" in _summary.lower():
                db.set_state("api_outlook_cache", None)
                logger.info("Startup: cleared stale forecast fallback from cache")
    except Exception:
        pass

# Timestamp of the last force-run on /api/eval/truth.
# Guards against concurrent/repeated calls saturating all gunicorn threads.
_eval_force_last: Optional[datetime] = None
_EVAL_FORCE_COOLDOWN_SECONDS = 300  # 5 minutes

# Feed cache: avoids re-running generate_stories on every 30-second poll.
_FEED_CACHE_TTL = 60  # seconds — JS polls every 30s so max staleness is ~90s


def _attach_sparklines(items: List[Any]) -> List[Dict]:
    """
    Convert story/cluster objects to dicts and attach sparkline history.

    For single stories: query by market_name.
    For clusters:       query the top-probability market's name.
    """
    dicts = [item.to_dict() for item in items]

    # Collect all market names we need history for
    name_to_idx: Dict[str, List[int]] = {}
    for i, (item, d) in enumerate(zip(items, dicts)):
        from story_generator import StoryCluster
        if isinstance(item, StoryCluster):
            key = item.stories[0].market_name   # top-probability market
        else:
            key = item.market_name
        name_to_idx.setdefault(key, []).append(i)

    all_names = list(name_to_idx.keys())
    history = db.get_price_history_batch(all_names, hours=24, max_points=40)

    for name, indices in name_to_idx.items():
        pts = history.get(name, [])
        for i in indices:
            dicts[i]["sparkline"] = pts

    return dicts


@app.route("/health")
def health():
    """
    Ultra-lightweight health check for Render.
    Returns 200 with no DB calls, no computation — just proof the
    Python process is alive and accepting HTTP requests.
    Render's health check has a 5-second timeout; this responds in <1ms.
    """
    return '{"status":"ok"}', 200, {"Content-Type": "application/json"}


@app.route("/")
def brief():
    return render_template("brief.html")


@app.route("/markets")
def markets():
    return render_template("index.html")


@app.route("/whales")
def whales():
    return render_template("whales.html")


@app.route("/resolved")
def resolved():
    return render_template("resolved.html")


@app.route("/forecast")
def forecast():
    return render_template("outlook.html")


@app.route("/outlook")
def outlook_redirect():
    """Redirect old /outlook URL to /forecast (avoids Chrome Safe Browsing flag)."""
    from flask import redirect
    return redirect("/forecast", code=301)


@app.route("/eval")
def eval_dashboard():
    return render_template("eval.html")


_WHALES_CACHE_TTL = 600  # 10 minutes — matches whale_intelligence.py CACHE_TTL


@app.route("/api/debug/cache")
def api_debug_cache():
    """Temporary diagnostic endpoint — shows cache state for all slow endpoints."""
    import traceback as _tb

    results = {}
    for key in ("api_feed_cache", "api_whales_cache", "api_resolved_cache", "api_outlook_cache"):
        try:
            cached = db.get_state(key, default=None)
            if cached and isinstance(cached, dict):
                ts = cached.get("ts", "?")
                data = cached.get("data", {})
                # Summarise the payload size without dumping everything
                if isinstance(data, dict):
                    summary = {k: type(v).__name__ + (f"[{len(v)}]" if isinstance(v, (list, dict)) else "")
                               for k, v in data.items()}
                else:
                    summary = str(type(data))
                results[key] = {"ts": ts, "summary": summary}
            else:
                results[key] = None
        except Exception as exc:
            results[key] = {"error": str(exc)}

    # Also try a quick smoke-test of each computation to expose errors
    errors = {}
    for label, fn in [
        ("resolved_db_query", lambda: len(db.get_resolved_context_markets(limit=2))),
        ("whales_db_query",   lambda: len(db.get_recent_alerts_feed(hours=24, limit=5))),
        ("top_volume_query",  lambda: len(db.get_top_volume_markets(limit=5, hours=2))),
    ]:
        try:
            errors[label] = {"ok": True, "rows": fn()}
        except Exception as exc:
            errors[label] = {"ok": False, "error": str(exc), "tb": _tb.format_exc()[-500:]}

    # Read any errors stored by background threads
    bg_errors = {}
    for key in ("_debug_whales_error", "_debug_resolved_error", "_debug_outlook_error"):
        try:
            val = db.get_state(key, default=None)
            if val:
                bg_errors[key] = val
        except Exception:
            pass

    return jsonify({"caches": results, "smoke_tests": errors, "bg_errors": bg_errors, "claude_active": _claude_active})


@app.route("/api/whales")
def api_whales():
    """
    Whale intelligence endpoint — returns large prediction market traders
    with Claude-generated intelligence briefs.

    Uses stale-while-revalidate: serves cached data instantly, refreshes
    in background.  First-ever request returns an empty shell so the
    browser never hangs waiting for a 90-second compute.
    """
    import threading

    try:
        limit = min(int(request.args.get("limit", 10)), 20)
    except (TypeError, ValueError):
        limit = 10

    def _empty_payload():
        return {
            "market_flows":    [],
            "whale_profiles":  [],
            "recent_trades":   [],
            "evidence_trades": [],
            "synthesis": {
                "brief": {
                    "equity_bias":       "MIXED",
                    "risk_appetite":     "NEUTRAL",
                    "geopolitical_risk": "MODERATE",
                    "confidence":        0,
                    "time_horizon":      "DAYS",
                    "synthesis":         "Scanning Polymarket for whale activity — intelligence brief generating…",
                },
                "lenses":    [],
                "clusters":  [],
                "consensus": [],
                "tensions":  [],
            },
            "stats": {
                "total_whales":      0,
                "total_flow_volume": 0,
                "top_insider_score": 0,
                "markets_with_flow": 0,
                "trades_scanned":    0,
            },
            "claude_active": _claude_active,
            "server_time":  datetime.now(timezone.utc).isoformat(),
        }

    def _compute_and_cache():
        import concurrent.futures
        try:
            # Run whale discovery with a hard 120s timeout so the thread
            # doesn't hang forever on Polymarket API slowness.
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(whale_brain.generate_whale_intelligence, limit=limit)
                payload = future.result(timeout=120)
            db.set_state("api_whales_cache", {
                "ts":   datetime.now(timezone.utc).isoformat(),
                "data": payload,
            })
            n_profiles = len(payload.get("whale_profiles", []))
            n_flows = len(payload.get("market_flows", []))
            logger.info(f"Whales cache refreshed: {n_profiles} profiles, {n_flows} market flows")
        except concurrent.futures.TimeoutError:
            logger.error("Whales cache refresh timed out after 120s")
            try: db.set_state("_debug_whales_error", {"error": "Timeout after 120s", "ts": datetime.now(timezone.utc).isoformat()})
            except Exception: pass
        except Exception as exc:
            import traceback; logger.error(f"Whales cache refresh failed: {exc}")
            try: db.set_state("_debug_whales_error", {"error": str(exc), "tb": traceback.format_exc()[-800:], "ts": datetime.now(timezone.utc).isoformat()})
            except Exception: pass

    cached = db.get_state("api_whales_cache", default=None)
    if cached:
        try:
            ts = datetime.fromisoformat(cached["ts"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - ts).total_seconds()
            if age < _WHALES_CACHE_TTL:
                return jsonify(cached["data"])
            # Stale — serve stale data, refresh in background
            threading.Thread(target=_compute_and_cache, daemon=True).start()
            return jsonify(cached["data"])
        except Exception:
            pass

    # No cache at all — return empty shell, compute in background
    threading.Thread(target=_compute_and_cache, daemon=True).start()
    return jsonify(_empty_payload())


_OUTLOOK_CACHE_TTL = 600  # 10 minutes — outlook is a large Claude call


@app.route("/api/forecast")
def api_forecast():
    """
    Asset price prediction endpoint — Claude Sonnet synthesizes all signals
    into directional predictions with magnitude and confidence scores.

    Uses stale-while-revalidate: serves cached data instantly, refreshes
    in background.  First-ever request returns the template fallback.
    """
    import threading

    force = request.args.get("force", "").lower() in ("1", "true", "yes")

    def _compute_and_cache():
        import concurrent.futures
        try:
            def _do_compute():
                # Use new deterministic ForecastEngine
                d = forecast_engine.generate(db)
                try:
                    d["live_prices"] = outlook_grader.get_live_price_snapshot()
                except Exception:
                    d["live_prices"] = {
                        "captured_at": datetime.now(timezone.utc).isoformat(),
                        "source": "yfinance", "assets": {},
                        "summary": {"live": 0, "delayed": 0, "stale": 0, "missing": 0},
                    }
                return d

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(_do_compute)
                data = future.result(timeout=90)

            db.set_state("api_outlook_cache", {
                "ts":   datetime.now(timezone.utc).isoformat(),
                "data": data,
            })
            logger.info("Forecast cache refreshed (deterministic engine)")
        except concurrent.futures.TimeoutError:
            logger.error("Forecast cache refresh timed out after 90s")
            try: db.set_state("_debug_outlook_error", {"error": "Timeout after 90s", "ts": datetime.now(timezone.utc).isoformat()})
            except Exception: pass
        except Exception as exc:
            import traceback; logger.error(f"Forecast cache refresh failed: {exc}")
            try: db.set_state("_debug_outlook_error", {"error": str(exc), "tb": traceback.format_exc()[-800:], "ts": datetime.now(timezone.utc).isoformat()})
            except Exception: pass

    cached = db.get_state("api_outlook_cache", default=None)
    if cached and not force:
        try:
            ts = datetime.fromisoformat(cached["ts"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - ts).total_seconds()
            if age < _OUTLOOK_CACHE_TTL:
                return jsonify(cached["data"])
            # Stale — serve stale data, refresh in background
            threading.Thread(target=_compute_and_cache, daemon=True).start()
            return jsonify(cached["data"])
        except Exception:
            pass

    # No cache (or force) — return loading skeleton, compute in background
    threading.Thread(target=_compute_and_cache, daemon=True).start()
    fallback = forecast_engine.fallback(
        reason="Generating forecast — computing signals across all assets."
    )
    fallback["live_prices"] = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "source": "yfinance", "assets": {},
        "summary": {"live": 0, "delayed": 0, "stale": 0, "missing": 0},
    }
    return jsonify(fallback)


@app.route("/api/forecast/track-record")
def api_forecast_track_record():
    """
    Track Record endpoint — grades past Outlook predictions vs actual prices,
    returns per-asset accuracy stats, grade history, and Claude's reflection.
    Triggers on-demand grading of any pending predictions.
    """
    payload = outlook_grader.get_track_record(db)
    return jsonify(payload)


@app.route("/api/forecast/evaluation")
def api_forecast_evaluation():
    """
    Forecast evaluation endpoint — returns signal weights, driver quality,
    calibration data, Brier scores, and baseline comparison.
    """
    payload = forecast_evaluator.get_evaluation(db)
    return jsonify(payload)


_RESOLVED_CACHE_TTL = 1800  # 30 minutes — resolved markets change slowly


@app.route("/api/resolved")
def api_resolved():
    """
    Resolved Context endpoint — settled high-volume markets with Claude
    explanations of what happened and live descendant markets to watch.

    Uses stale-while-revalidate: serves cached data instantly, refreshes
    in background.  First-ever request returns an empty shell.
    """
    import threading

    def _empty_payload():
        return {
            "cards":        [],
            "total":        0,
            "claude_active": _claude_active,
            "server_time":  datetime.now(timezone.utc).isoformat(),
        }

    def _compute_and_cache():
        import concurrent.futures
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(story_gen.generate_resolved_context, db, limit=6)
                cards = future.result(timeout=90)
            payload = {
                "cards":        cards,
                "total":        len(cards),
                "claude_active": _claude_active,
                "server_time":  datetime.now(timezone.utc).isoformat(),
            }
            db.set_state("api_resolved_cache", {
                "ts":   datetime.now(timezone.utc).isoformat(),
                "data": payload,
            })
            logger.info(f"Resolved cache refreshed: {len(cards)} cards")
        except concurrent.futures.TimeoutError:
            logger.error("Resolved cache refresh timed out after 90s")
            try: db.set_state("_debug_resolved_error", {"error": "Timeout after 90s", "ts": datetime.now(timezone.utc).isoformat()})
            except Exception: pass
        except Exception as exc:
            import traceback; logger.error(f"Resolved cache refresh failed: {exc}")
            try: db.set_state("_debug_resolved_error", {"error": str(exc), "tb": traceback.format_exc()[-800:], "ts": datetime.now(timezone.utc).isoformat()})
            except Exception: pass

    cached = db.get_state("api_resolved_cache", default=None)
    if cached:
        try:
            ts = datetime.fromisoformat(cached["ts"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - ts).total_seconds()
            if age < _RESOLVED_CACHE_TTL:
                return jsonify(cached["data"])
            # Stale — serve stale data, refresh in background
            threading.Thread(target=_compute_and_cache, daemon=True).start()
            return jsonify(cached["data"])
        except Exception:
            pass

    # No cache at all — return empty shell, compute in background
    threading.Thread(target=_compute_and_cache, daemon=True).start()
    return jsonify(_empty_payload())


@app.route("/api/feed")
def api_feed():
    """
    Primary API endpoint — returns stories, radar items, and system stats.
    Polled by the dashboard JS every 30 seconds.

    Responses are cached in the DB state table for _FEED_CACHE_TTL seconds.
    A background thread refreshes the cache so callers never wait on live
    Claude / DB computation.
    """
    import threading

    # ── Serve from cache if fresh ─────────────────────────────────────────
    cached = db.get_state("api_feed_cache", default=None)
    if cached:
        try:
            ts = datetime.fromisoformat(cached["ts"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - ts).total_seconds()
            if age < _FEED_CACHE_TTL:
                return jsonify(cached["data"])
            # Cache stale — serve stale data immediately while refreshing in bg
            def _refresh():
                try:
                    stories = story_gen.generate_stories(db, hours=24, limit=40)
                    radar   = story_gen.generate_radar(db, hours=24, limit=20)
                    stats   = db.get_system_stats()
                    payload = {
                        "stories":       _attach_sparklines(stories),
                        "radar":         _attach_sparklines(radar),
                        "stats":         stats,
                        "claude_active": _claude_active,
                        "server_time":   datetime.now(timezone.utc).isoformat(),
                    }
                    db.set_state("api_feed_cache", {
                        "ts":   datetime.now(timezone.utc).isoformat(),
                        "data": payload,
                    })
                except Exception as exc:
                    logger.debug(f"Feed cache refresh failed: {exc}")
            threading.Thread(target=_refresh, daemon=True).start()
            return jsonify(cached["data"])
        except Exception:
            pass

    # ── Cache missing (first request after fresh deploy) ──────────────
    #    Return an empty shell immediately and compute in background.
    #    This prevents the first request from blocking a gunicorn worker
    #    for 30+ seconds, which causes cascading health check failures.
    def _build_and_cache():
        try:
            stories = story_gen.generate_stories(db, hours=24, limit=40)
            radar   = story_gen.generate_radar(db, hours=24, limit=20)
            stats   = db.get_system_stats()
            payload = {
                "stories":       _attach_sparklines(stories),
                "radar":         _attach_sparklines(radar),
                "stats":         stats,
                "claude_active": _claude_active,
                "server_time":   datetime.now(timezone.utc).isoformat(),
            }
            db.set_state("api_feed_cache", {
                "ts":   datetime.now(timezone.utc).isoformat(),
                "data": payload,
            })
            logger.info(f"Feed cache built: {len(stories)} stories, {len(radar)} radar")
        except Exception as exc:
            logger.error(f"Feed cache build failed: {exc}")
    threading.Thread(target=_build_and_cache, daemon=True).start()
    return jsonify({
        "stories":       [],
        "radar":         [],
        "stats":         {"markets_active": 0, "signals_24h": 0, "last_update": None},
        "claude_active": _claude_active,
        "server_time":   datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/stats")
def api_stats():
    try:
        stats = db.get_system_stats()
    except Exception as exc:
        logger.warning(f"/api/stats DB query failed: {exc}")
        return jsonify({"markets_active": 0, "signals_24h": 0, "last_update": None, "monitor_stale": True})
    # Surface data-freshness so the platform can detect a dead monitor
    # process (not just a dead web process).
    last_update = stats.get("last_update")
    stale = True
    if last_update:
        try:
            lu = datetime.fromisoformat(last_update)
            if lu.tzinfo is None:
                lu = lu.replace(tzinfo=timezone.utc)
            stale = (datetime.now(timezone.utc) - lu).total_seconds() > 600  # >10 min = stale
        except Exception:
            pass
    stats["monitor_stale"] = stale
    return jsonify(stats)


@app.route("/api/eval/truth")
def api_eval_truth():
    """
    Truth-engine endpoint:
      - alert and move labeling metrics
      - precision/recall slices
      - calibration curves + error
      - weekly trend
    Query params:
      force=1   -> recompute on demand before returning
      days=30   -> lookback window
    """
    try:
        lookback_days = max(7, min(180, int(request.args.get("days", 30))))
    except (TypeError, ValueError):
        lookback_days = 30

    global _eval_force_last
    force = request.args.get("force", "").lower() in ("1", "true", "yes")
    if force:
        now = datetime.now(timezone.utc)
        if (
            _eval_force_last is not None
            and (now - _eval_force_last).total_seconds() < _EVAL_FORCE_COOLDOWN_SECONDS
        ):
            remaining = int(_EVAL_FORCE_COOLDOWN_SECONDS - (now - _eval_force_last).total_seconds())
            return jsonify({"error": f"force-run cooldown active, retry in {remaining}s"}), 429
        _eval_force_last = now
        db.detect_market_move_events(
            window_minutes=60,
            min_change_pp=2.0,
            scan_minutes=min(lookback_days * 24 * 60, 60 * 24 * 30),
            per_market_cooldown_minutes=20,
        )
        db.label_alert_outcomes(horizon_minutes=180, success_move_pp=3.0, limit=5000)
        db.label_market_move_outcomes(horizon_minutes=180, success_move_pp=2.5, limit=8000)
        report = db.get_truth_engine_report(lookback_days=lookback_days, min_samples=5)
        db.set_state("truth_engine_report", report)
    else:
        report = db.get_state("truth_engine_report", default=None)
        if not report:
            report = db.get_truth_engine_report(lookback_days=lookback_days, min_samples=5)

    recent_moves = db.get_recent_move_events(hours=72, limit=80)
    return jsonify({
        "report": report,
        "recent_moves": recent_moves,
        "server_time": datetime.now(timezone.utc).isoformat(),
    })


# ---------------------------------------------------------------------------
# Helpers for the /api/context endpoint
# ---------------------------------------------------------------------------

_CTX_STOP = frozenset([
    'a', 'an', 'the', 'is', 'are', 'will', 'would', 'could', 'should',
    'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or', 'but', 'by',
    'from', 'with', 'be', 'been', 'have', 'has', 'had', 'was', 'were',
    'this', 'that', 'which', 'who', 'what', 'when', 'where', 'if', 'than',
    'as', 'do', 'does', 'did', 'how', 'why', 'not', 'no', 'more', 'most',
    'before', 'after', 'during', 'between', 'over', 'under', 'about', 'up',
    'out', 'it', 'its', 'get', 'got', 'also', 'than', 'just', 'into',
])


def _extract_search_terms(market_name: str, max_terms: int = 6) -> List[str]:
    """
    Pull the most distinctive content words from a market name to use
    as news-cache search terms.

    Prioritises:
      1. Capitalised proper-noun tokens (names, places, orgs)
      2. Other non-stop content words

    Returns at most `max_terms` lower-cased strings.
    """
    # Strip leading "Will / Does / Is / Are…" question structure
    cleaned = re.sub(
        r'^(will|does|is|are|who will|what will|when will|can|has|have)\s+',
        '', market_name.strip(), flags=re.IGNORECASE
    )
    cleaned = re.sub(r'\?$', '', cleaned).strip()

    tokens = re.findall(r"[A-Za-z]{3,}", cleaned)

    proper = [t for t in tokens if t[0].isupper() and t.lower() not in _CTX_STOP]
    rest   = [t for t in tokens if t[0].islower() and t.lower() not in _CTX_STOP]

    ordered = proper + rest
    seen: Dict[str, bool] = {}
    result = []
    for t in ordered:
        k = t.lower()
        if k not in seen:
            seen[k] = True
            result.append(k)
        if len(result) >= max_terms:
            break

    return result


def _content_words(text: str) -> List[str]:
    tokens = re.findall(r"[a-zA-Z]{3,}", (text or "").lower())
    return [t for t in tokens if t not in _CTX_STOP]


def _infer_why_now(signals: List[str], prob_change: Optional[float], news_count: int) -> str:
    signal_blob = " ".join(signals).lower()
    if "whale" in signal_blob:
        return "Large-wallet flow has appeared ahead of broader repricing, suggesting informed positioning."
    if "cross-market" in signal_blob or "gap" in signal_blob or "divergence" in signal_blob:
        return "Cross-venue disagreement widened, indicating a fast information transmission gap between platforms."
    if "odd-hour" in signal_blob or "off-peak" in signal_blob:
        return "Activity spiked during off-peak hours, a pattern often associated with event-driven positioning."
    if "no_news" in signal_blob or ("zero news" in signal_blob):
        return "Price moved despite thin news coverage, which increases the odds of private or anticipatory information flow."
    if prob_change is not None and abs(prob_change) >= 8:
        return "A large move happened in a short window, pushing this market from noise into actionable signal territory."
    if news_count > 0:
        return "Fresh headline flow and market flow are now aligned, increasing confidence that this is information-driven."
    return "Multiple independent micro-signals aligned in this cycle, elevating this event above baseline market noise."


def _confidence_decomposition(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Deterministic confidence decomposition for UI transparency.
    Components sum to <= signal_score.
    """
    score = float(item.get("signal_score", 0.0) or 0.0)
    signal_types = [str(s) for s in (item.get("signal_types") or [])]
    prob_change = abs(float(item.get("prob_change", 0.0) or 0.0))

    weights = {
        "price_velocity": 0.25,
        "volume_shock": 0.18,
        "thin_liquidity_jump": 0.12,
        "cross_market_divergence": 0.16,
        "odd_hour_activity": 0.10,
        "acceleration": 0.10,
        "orderbook_imbalance": 0.12,
        "no_news_move": 0.12,
        "whale_activity": 0.15,
        "radar_momentum": 0.08,
    }
    components: List[Dict[str, Any]] = []
    used = 0.0

    for signal_type in signal_types[:6]:
        w = weights.get(signal_type, 0.08)
        contribution = round(min(score * w, score - used), 2)
        if contribution <= 0:
            continue
        components.append({
            "component": signal_type,
            "points": contribution,
            "rationale": f"{signal_type.replace('_', ' ')} contributed directly to this alert score.",
        })
        used += contribution
        if used >= score:
            break

    move_bonus = min(6.0, prob_change * 0.4)
    remaining = max(0.0, score - used)
    if remaining > 0 and move_bonus > 0:
        bonus = round(min(move_bonus, remaining), 2)
        components.append({
            "component": "price_regime",
            "points": bonus,
            "rationale": f"Absolute move size ({prob_change:.1f}pp) increased confidence in signal durability.",
        })
        used += bonus

    residual = round(max(0.0, score - used), 2)
    if residual > 0:
        components.append({
            "component": "base_context",
            "points": residual,
            "rationale": "Residual score reflects baseline heuristics and category-level priors.",
        })
    return components


def _find_historical_analogs(item: Dict[str, Any], max_results: int = 4) -> List[Dict[str, Any]]:
    market_name = item.get("market_name", "")
    category = (item.get("category") or "").strip().lower()
    target_words = set(_content_words(market_name))
    candidates = db.get_recent_alert_candidates(category=category or None, days=180, limit=600)

    scored: List[Dict[str, Any]] = []
    for row in candidates:
        if row.get("market_name") == market_name:
            continue
        words = set(_content_words(row.get("market_name", "")))
        if not words or not target_words:
            continue
        overlap = len(target_words & words) / max(1, min(len(target_words), len(words)))
        if overlap < 0.28:
            continue
        outcome = row.get("outcome_label")
        scored.append({
            "market_name": row.get("market_name"),
            "platform": row.get("platform"),
            "timestamp": row.get("timestamp"),
            "similarity": round(overlap, 3),
            "outcome_label": outcome,
            "outcome_magnitude": row.get("outcome_magnitude"),
            "time_to_hit_minutes": row.get("time_to_hit_minutes"),
            "signal_score": row.get("signal_score"),
            "signal_types": json.loads(row.get("signal_types") or "[]"),
        })

    scored.sort(
        key=lambda r: (r["similarity"], r.get("signal_score") or 0),
        reverse=True,
    )
    return scored[:max_results]


def _build_thesis_key(item: Dict[str, Any]) -> str:
    market = item.get("market_name", "")
    category = item.get("category", "other")
    words = _content_words(market)[:6]
    base = f"{category.lower()}:{' '.join(words)}".strip(":")
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:10]
    return f"{category.lower()}-{digest}"


def _normalize_workflow_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize dashboard payloads (single stories, radar cards, clusters)
    so workflow/thesis/watchlist actions always receive market identifiers.
    """
    item = dict(raw_item or {})
    if not item:
        return {}

    cluster_markets = item.get("cluster_markets")
    first_cluster = None
    if isinstance(cluster_markets, list) and cluster_markets:
        first_cluster = cluster_markets[0] if isinstance(cluster_markets[0], dict) else None

    if not item.get("market_name") and first_cluster:
        item["market_name"] = str(first_cluster.get("market_name") or "").strip()
    if not item.get("market_id"):
        fallback_id = first_cluster.get("market_id") if first_cluster else None
        item["market_id"] = str(fallback_id or item.get("id") or item.get("market_name") or "").strip()
    if not item.get("platform") and first_cluster:
        item["platform"] = first_cluster.get("platform")
    if not item.get("platform"):
        item["platform"] = "polymarket"
    item["platform"] = str(item.get("platform") or "polymarket").strip().lower()

    if item.get("probability") is None and first_cluster and first_cluster.get("probability") is not None:
        item["probability"] = first_cluster.get("probability")
    if item.get("old_probability") is None and first_cluster and first_cluster.get("old_probability") is not None:
        item["old_probability"] = first_cluster.get("old_probability")
    if item.get("prob_change") is None and first_cluster and first_cluster.get("prob_change") is not None:
        item["prob_change"] = first_cluster.get("prob_change")

    if not item.get("category"):
        item["category"] = "OTHER"
    item["category"] = str(item.get("category") or "OTHER").strip().upper()

    signal_types = item.get("signal_types")
    if not isinstance(signal_types, list):
        signal_types = []
    signals = item.get("signals")
    if not isinstance(signals, list):
        signals = []

    item["signal_types"] = [str(s) for s in signal_types if s is not None]
    item["signals"] = [str(s) for s in signals if s is not None]
    item["market_name"] = str(item.get("market_name") or "").strip()
    return item


@app.route("/api/context")
def api_context():
    """
    Per-story Intelligence Note endpoint.

    Query params:
      market   — full market name
      prob     — current probability (0-100)
      change   — probability change (signed float, optional)
      platform — 'polymarket' or 'kalshi'
      signals  — pipe-separated list of detected signal strings

    Returns:
      { analysis: str|null, news: [{title,source,url}], generated: bool }
    """
    market   = request.args.get("market", "").strip()
    platform = request.args.get("platform", "").strip()

    try:
        prob   = float(request.args.get("prob", 50))
    except (TypeError, ValueError):
        prob   = 50.0

    raw_change = request.args.get("change", "")
    try:
        change: Optional[float] = float(raw_change) if raw_change else None
    except (TypeError, ValueError):
        change = None

    raw_signals = request.args.get("signals", "")
    signals = [s.strip() for s in raw_signals.split("|") if s.strip()] if raw_signals else []

    if not market:
        return jsonify({"analysis": None, "news": [], "generated": False}), 400

    # ── Search news cache ──────────────────────────────────────────────
    terms    = _extract_search_terms(market)
    articles = db.search_recent_news(terms, hours=48) if terms else []
    news_out = [
        {"title": a["title"], "source": a.get("source", ""), "url": a.get("url", "")}
        for a in articles[:5]
    ]

    # ── Claude analysis ────────────────────────────────────────────────
    analysis = None
    generated = False

    if _claude_active and story_gen._claude:
        try:
            result    = story_gen._claude.analyze_context(
                market_name=market,
                prob=prob,
                change=change,
                platform=platform,
                signals=signals,
                news_articles=articles[:5],
            )
            analysis  = result.get("analysis") or None
            generated = bool(analysis)
        except Exception as e:
            logger.debug(f"Context analysis failed: {e}")

    return jsonify({
        "analysis":  analysis,
        "news":      news_out,
        "generated": generated,
    })


@app.route("/api/workflow/context", methods=["POST"])
def api_workflow_context():
    """
    Build decision-workflow context for a selected alert/radar item.
    Returns:
      why_now, what_changed, historical_analogs, confidence_decomposition,
      falsifiers, scenario_tree, next_best_actions
    """
    payload = request.get_json(silent=True) or {}
    raw_item = payload.get("item") if isinstance(payload.get("item"), dict) else payload
    item = _normalize_workflow_item(raw_item if isinstance(raw_item, dict) else {})

    market_name = item.get("market_name", "")
    if not market_name:
        return jsonify({"error": "market_name required"}), 400

    def _to_float(v: Any) -> Optional[float]:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    prob = _to_float(item.get("probability"))
    if prob is None:
        prob = 50.0
    old_prob_f = _to_float(item.get("old_probability"))
    prob_change = item.get("prob_change")
    prob_change_f = _to_float(prob_change) if prob_change is not None else (
        (prob - old_prob_f) if old_prob_f is not None else None
    )
    signal_types = [str(s) for s in (item.get("signal_types") or [])]
    signals = [str(s) for s in ((item.get("signals") or []) + signal_types)]

    terms = _extract_search_terms(market_name)
    news = db.search_recent_news(terms, hours=48) if terms else []
    why_now = _infer_why_now(signals, prob_change_f, len(news))

    what_changed = {
        "probability_now": prob,
        "probability_before": old_prob_f,
        "delta_pp": round(prob_change_f, 2) if prob_change_f is not None else None,
        "signal_count": len(signals),
        "new_signal_types": [str(s) for s in (item.get("signal_types") or [])],
        "summary": (
            f"Probability moved {prob_change_f:+.1f}pp to {prob:.1f}% "
            if prob_change_f is not None
            else f"Current probability is {prob:.1f}% "
        ) + f"with {len(signals)} active signal(s).",
    }

    analogs = _find_historical_analogs(item, max_results=4)
    confidence = _confidence_decomposition(item)
    hit_count = sum(1 for a in analogs if a.get("outcome_label") == 1)
    labeled_analogs = sum(1 for a in analogs if a.get("outcome_label") in (0, 1))
    analog_hit_rate = (hit_count / labeled_analogs) if labeled_analogs else None

    direction = 1
    if prob_change_f is not None and prob_change_f < 0:
        direction = -1
    elif prob_change_f is not None and prob_change_f > 0:
        direction = 1
    elif prob < 50:
        direction = -1

    invalidation_level = round(
        max(1.0, min(99.0, prob - 6.0 if direction > 0 else prob + 6.0)),
        1,
    )
    recapture_level = round(
        max(1.0, min(99.0, prob + 6.0 if direction > 0 else prob - 6.0)),
        1,
    )
    falsifiers = [
        {
            "condition": f"Probability crosses {invalidation_level:.1f}% against thesis direction for two consecutive updates.",
            "why": "Sustained adverse repricing usually indicates thesis breakdown, not short-term noise.",
        },
        {
            "condition": "Next 3 related signals average below 45 signal score.",
            "why": "Weak follow-through implies the initial edge is decaying.",
        },
        {
            "condition": f"Post-catalyst price fails to hold near {recapture_level:.1f}% threshold.",
            "why": "Failed hold after catalyst often precedes mean reversion.",
        },
    ]

    if direction > 0:
        scenario_tree = [
            {"scenario": "Confirm", "trigger": "Catalyst confirms; signal quality stays strong.", "range": [round(min(99.0, prob + 6.0), 1), round(min(99.0, prob + 16.0), 1)], "implication": "Lean into thesis quickly."},
            {"scenario": "Base", "trigger": "Mixed data, no strong surprise.", "range": [round(max(1.0, prob - 4.0), 1), round(min(99.0, prob + 6.0), 1)], "implication": "Wait for cleaner confirmation."},
            {"scenario": "Invalidate", "trigger": "Adverse catalyst or flow reversal.", "range": [round(max(1.0, prob - 18.0), 1), round(max(1.0, prob - 6.0), 1)], "implication": "Exit thesis and rotate."},
        ]
    else:
        scenario_tree = [
            {"scenario": "Confirm", "trigger": "Catalyst confirms downside thesis.", "range": [round(max(1.0, prob - 16.0), 1), round(max(1.0, prob - 6.0), 1)], "implication": "Lean into thesis quickly."},
            {"scenario": "Base", "trigger": "Mixed data, no strong surprise.", "range": [round(max(1.0, prob - 6.0), 1), round(min(99.0, prob + 4.0), 1)], "implication": "Wait for cleaner confirmation."},
            {"scenario": "Invalidate", "trigger": "Positive surprise or sharp reversal.", "range": [round(min(99.0, prob + 6.0), 1), round(min(99.0, prob + 18.0), 1)], "implication": "Exit thesis and rotate."},
        ]

    urgency_score = float(item.get("signal_score") or 0.0) * 0.55 + min(22.0, abs(prob_change_f or 0.0) * 2.8)
    urgency_score += 10.0 if len(news) > 0 else 0.0
    urgency_score = max(0.0, min(100.0, urgency_score))
    if urgency_score >= 78:
        decision_sla_minutes = 30
    elif urgency_score >= 62:
        decision_sla_minutes = 90
    elif urgency_score >= 45:
        decision_sla_minutes = 240
    else:
        decision_sla_minutes = 720

    top_signal = signal_types[0].replace("_", " ") if signal_types else "primary catalyst"
    next_best_actions = [
        {
            "priority": 1,
            "action": f"Set invalidation guardrail at {invalidation_level:.1f}%.",
            "why": "Pre-committed risk rules reduce reaction-time slippage.",
            "eta_minutes": 10,
        },
        {
            "priority": 2,
            "action": f"Monitor {top_signal} follow-through in next cycle.",
            "why": "Immediate follow-through determines whether edge is durable.",
            "eta_minutes": min(120, decision_sla_minutes),
        },
        {
            "priority": 3,
            "action": "Compare with nearest historical analog before increasing conviction.",
            "why": f"Labeled analog hit-rate is {analog_hit_rate:.0%}." if analog_hit_rate is not None else "Analog outcomes calibrate overconfidence.",
            "eta_minutes": 20,
        },
    ]

    return jsonify({
        "why_now": why_now,
        "what_changed": what_changed,
        "historical_analogs": analogs,
        "confidence_decomposition": confidence,
        "falsifiers": falsifiers,
        "scenario_tree": scenario_tree,
        "next_best_actions": next_best_actions,
        "decision_sla_minutes": decision_sla_minutes,
    })


@app.route("/api/watchlists", methods=["GET", "POST"])
def api_watchlists():
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        name = (payload.get("name") or "Default").strip()[:80] or "Default"
        db.ensure_watchlist(name)
    return jsonify({"watchlists": db.get_watchlists()})


@app.route("/api/watchlists/enriched")
def api_watchlists_enriched():
    try:
        limit = max(5, min(80, int(request.args.get("items", 40) or 40)))
    except (TypeError, ValueError):
        limit = 40
    watchlists = db.get_watchlists_enriched(max_items_per_watchlist=limit)
    return jsonify({"watchlists": watchlists})


@app.route("/api/watchlists/items", methods=["POST", "DELETE"])
def api_watchlist_items():
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        watchlist_name = (payload.get("watchlist_name") or "Default").strip()[:80] or "Default"
        market_id = str(payload.get("market_id") or "").strip()
        market_name = str(payload.get("market_name") or "").strip()
        platform = str(payload.get("platform") or "").strip().lower()
        category = str(payload.get("category") or "").strip()
        notes = str(payload.get("notes") or "").strip()
        if not (market_id and market_name and platform):
            return jsonify({"ok": False, "error": "market_id, market_name, platform required"}), 400
        ok = db.add_watchlist_item(
            watchlist_name=watchlist_name,
            market_id=market_id,
            market_name=market_name,
            platform=platform,
            category=category,
            notes=notes,
        )
        return jsonify({"ok": ok})

    # item_id can come from JSON body or query param (DELETE requests may have body stripped by proxies)
    payload = request.get_json(silent=True) or {}
    item_id = payload.get("item_id") or request.args.get("item_id")
    if item_id is None:
        return jsonify({"ok": False, "error": "item_id required"}), 400
    ok = db.remove_watchlist_item(int(item_id))
    return jsonify({"ok": ok})


@app.route("/api/thesis", methods=["GET", "POST"])
def api_thesis():
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        raw_item = payload.get("item") if isinstance(payload.get("item"), dict) else payload
        item = _normalize_workflow_item(raw_item if isinstance(raw_item, dict) else {})
        market_name = str(item.get("market_name") or "").strip()
        if not market_name:
            return jsonify({"ok": False, "error": "market_name required"}), 400

        category = str(item.get("category") or "OTHER").strip().upper()
        thesis_key = str(payload.get("thesis_key") or _build_thesis_key(item))
        title = str(payload.get("title") or f"{category.title()} Thesis: {market_name[:72]}").strip()
        note = str(payload.get("note") or "Started following this thesis from dashboard workflow.").strip()

        db.follow_thesis(
            thesis_key=thesis_key,
            title=title,
            category=category,
            note=note,
            payload={
                "market_name": market_name,
                "market_id": item.get("market_id"),
                "platform": item.get("platform"),
                "probability": item.get("probability"),
                "signal_score": item.get("signal_score"),
            },
        )
        return jsonify({"ok": True, "thesis_key": thesis_key})

    limit = min(int(request.args.get("limit", 12)), 50)
    return jsonify({"threads": db.get_thesis_threads(limit=limit)})


@app.route("/api/thesis/copilot")
def api_thesis_copilot():
    try:
        limit = max(1, min(50, int(request.args.get("limit", 12) or 12)))
    except (TypeError, ValueError):
        limit = 12
    try:
        lookback_days = max(7, min(120, int(request.args.get("days", 21) or 21)))
    except (TypeError, ValueError):
        lookback_days = 21
    threads = db.get_thesis_copilot_threads(limit=limit, alert_lookback_days=lookback_days)
    return jsonify({"threads": threads})


@app.route("/api/thesis/<thesis_key>/notes", methods=["POST"])
def api_thesis_note(thesis_key: str):
    payload = request.get_json(silent=True) or {}
    note = str(payload.get("note") or "").strip()
    if not note:
        return jsonify({"ok": False, "error": "note required"}), 400
    ok = db.add_thesis_note(thesis_key=thesis_key, note=note, payload=payload.get("payload") or {})
    return jsonify({"ok": ok})


@app.route("/api/thesis/<thesis_key>/actions", methods=["POST"])
def api_thesis_action(thesis_key: str):
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action") or "").strip()
    if not action:
        return jsonify({"ok": False, "error": "action required"}), 400
    rationale = str(payload.get("why") or payload.get("rationale") or "").strip()
    ok = db.add_thesis_action(
        thesis_key=thesis_key,
        action=action,
        rationale=rationale,
        payload=payload.get("payload") or {},
    )
    return jsonify({"ok": ok})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    public_hint = os.environ.get("RENDER_EXTERNAL_URL", f"http://localhost:{port}")
    logger.info("=" * 55)
    logger.info("Market Sentinel Dashboard starting...")
    logger.info("Open %s in your browser", public_hint)
    logger.info("=" * 55)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
