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
echo "Memory: $(free -m 2>/dev/null | head -2 || echo 'N/A')"

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
