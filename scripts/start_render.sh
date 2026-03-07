#!/usr/bin/env bash
set -uo pipefail  # Note: removed -e so background failures don't kill script

# Run the market monitor and web dashboard in one Render service so they
# share the same persistent SQLite disk.
#
# The monitor is backgrounded — if it crashes, the web server keeps running.
# The health check (/health) is DB-free so it never blocks on SQLite issues.

echo "=== Market Sentinel startup ==="
echo "Python: $(python3 --version 2>&1)"
echo "PORT: ${PORT:-not set}"
echo "DB path: ${SENTINEL_DB_PATH:-default}"
echo "CWD: $(pwd)"
echo "Memory: $(free -m 2>/dev/null | head -2 || echo 'N/A')"

# ── Pre-flight checks ──────────────────────────────────────────────
# Verify critical dependencies BEFORE trying to start gunicorn.
# This turns silent import errors into clear log messages.

echo ""
echo "--- Pre-flight checks ---"

# Check that gunicorn is installed
if python3 -c "import gunicorn; print(f'gunicorn {gunicorn.__version__}')" 2>/dev/null; then
    echo "  gunicorn: OK"
else
    echo "  gunicorn: MISSING — falling back to Flask dev server"
    GUNICORN_MISSING=1
fi

# Check that the app module can be imported
if python3 -c "from src.web_server import app; print(f'App routes: {len(list(app.url_map.iter_rules()))}')" 2>&1; then
    echo "  app import: OK"
else
    echo "  app import: FAILED"
    echo "  Attempting import with traceback..."
    python3 -c "from src.web_server import app" 2>&1 || true
fi

# Verify templates exist
for tpl in brief.html index.html whales.html resolved.html outlook.html eval.html; do
    if [ -f "dashboard/${tpl}" ]; then
        echo "  template ${tpl}: OK"
    else
        echo "  template ${tpl}: MISSING"
    fi
done

# Verify config
SENTINEL_CONFIG="${SENTINEL_CONFIG:-config.example.json}"
if [ -f "${SENTINEL_CONFIG}" ]; then
    echo "  config ${SENTINEL_CONFIG}: OK"
elif [ -f "config.example.json" ]; then
    echo "  config ${SENTINEL_CONFIG}: not found, but config.example.json exists"
else
    echo "  config: no config file found (will use defaults)"
fi

echo "--- Pre-flight complete ---"
echo ""

# ── Start monitor (background) ─────────────────────────────────────
echo "Starting market monitor (background)..."
python3 src/main.py >> /tmp/monitor.log 2>&1 &
MONITOR_PID=$!
echo "Monitor PID: ${MONITOR_PID}"

cleanup() {
  echo "Stopping monitor (PID ${MONITOR_PID})..."
  kill "${MONITOR_PID}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Brief pause to let DB initialize before gunicorn workers hit it
sleep 2

# ── Start web server ───────────────────────────────────────────────
if [ "${GUNICORN_MISSING:-}" = "1" ]; then
    echo "Starting Flask dev server (gunicorn unavailable)..."
    exec python3 -c "
from src.web_server import app
import os
port = int(os.environ.get('PORT', 5050))
app.run(host='0.0.0.0', port=port, threaded=True)
"
else
    echo "Starting gunicorn web server..."
    exec python3 -m gunicorn \
      --bind "0.0.0.0:${PORT}" \
      --workers 1 \
      --threads 4 \
      --timeout 120 \
      --graceful-timeout 30 \
      --keep-alive 5 \
      --max-requests 500 \
      --max-requests-jitter 50 \
      --preload \
      --access-logfile - \
      --error-logfile - \
      --log-level info \
      src.web_server:app
fi
