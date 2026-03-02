#!/usr/bin/env bash
set -euo pipefail

# Run the market monitor and web dashboard in one Render service so they
# share the same persistent SQLite disk.
python3 src/main.py &
MONITOR_PID=$!

cleanup() {
  kill "${MONITOR_PID}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

exec python3 -m gunicorn \
  --bind "0.0.0.0:${PORT}" \
  --workers 2 \
  --threads 4 \
  --timeout 180 \
  src.web_server:app
