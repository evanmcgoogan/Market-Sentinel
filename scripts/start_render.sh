#!/usr/bin/env bash
set -euo pipefail

# Run the market monitor and web dashboard in one Render service so they
# share the same persistent SQLite disk.
#
# The monitor is backgrounded — if it crashes, the web server keeps running.
# The health check (/health) is DB-free so it never blocks on SQLite issues.
echo "Starting market monitor (background)..."
python3 src/main.py &
MONITOR_PID=$!

cleanup() {
  kill "${MONITOR_PID}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Starting gunicorn web server..."

exec python3 -m gunicorn \
  --bind "0.0.0.0:${PORT}" \
  --workers 2 \
  --threads 4 \
  --timeout 120 \
  --graceful-timeout 30 \
  --keep-alive 5 \
  --max-requests 1000 \
  --max-requests-jitter 100 \
  --preload \
  --access-logfile - \
  --error-logfile - \
  --log-level info \
  src.web_server:app
