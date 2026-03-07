#!/usr/bin/env bash
# Market Sentinel — Render start script
#
# Kept deliberately minimal. Error handling lives in wsgi.py, not here.
# The wsgi.py wrapper catches import failures and serves a diagnostic page
# so gunicorn NEVER crashes due to app-level errors.

echo "=== Market Sentinel startup ==="
echo "Python: $(python3 --version 2>&1)"
echo "PORT:   ${PORT:-not set}"
echo "DB:     ${SENTINEL_DB_PATH:-default}"
echo "CWD:    $(pwd)"

# Start market monitor in background (non-fatal — if it crashes,
# the web server still runs and serves the dashboard).
python3 src/main.py >> /tmp/monitor.log 2>&1 &
echo "Monitor PID: $!"

# Let DB initialise before gunicorn workers connect
sleep 2

echo "Starting gunicorn..."
exec python3 -m gunicorn \
  --bind "0.0.0.0:${PORT:-5050}" \
  --workers 1 \
  --threads 4 \
  --timeout 120 \
  --graceful-timeout 30 \
  --keep-alive 5 \
  --max-requests 500 \
  --max-requests-jitter 50 \
  --access-logfile - \
  --error-logfile - \
  --log-level info \
  wsgi:app
