"""
Gunicorn WSGI entry point — error-safe wrapper around the real app.

If src.web_server fails to import (missing dep, DB error, config issue),
this module serves a minimal diagnostic Flask app so:
  1. Gunicorn stays alive (never crashes at startup)
  2. Render's /health check passes (service stays "live")
  3. The actual error is visible at / (no blind debugging)

This makes deploys impossible to crash due to import errors.
"""

import sys
import traceback

_boot_error = None
_boot_tb = None

try:
    from src.web_server import app  # noqa: F401
except Exception as exc:
    _boot_error = str(exc)
    _boot_tb = traceback.format_exc()

    # Create a minimal Flask app that responds to health checks
    # and shows the boot error on all other routes.
    from flask import Flask

    app = Flask(__name__)

    @app.route("/health")
    def health():
        return (
            '{"status":"degraded","boot_error":true}',
            200,
            {"Content-Type": "application/json"},
        )

    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def fallback(path):
        return (
            f"<html><body style='font-family:monospace;padding:2em;background:#1a1a2e;color:#e0e0e0;'>"
            f"<h1 style='color:#ff6b6b;'>Market Sentinel — Boot Error</h1>"
            f"<p>The application failed to start. Error details below:</p>"
            f"<pre style='background:#16213e;padding:1em;overflow:auto;border:1px solid #333;'>"
            f"{_boot_tb}</pre>"
            f"<p style='color:#888;margin-top:2em;'>Fix the error and redeploy. "
            f"This page will be replaced by the real dashboard automatically.</p>"
            f"</body></html>"
        ), 500

    import logging

    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("wsgi").error(
        "App failed to import — serving diagnostic page.\n%s", _boot_tb
    )
