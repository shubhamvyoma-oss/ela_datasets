#!/usr/bin/env bash
# edmingle_watchdog.sh
#
# Runs edmingle_export.py and automatically restarts it if it crashes
# (network drop, VPS hiccup, unhandled error, etc). Because the python
# script checkpoints its own progress, a restart resumes from the next
# page instead of starting over.
#
# Stops when:
#   - the python script exits 0 (finished successfully), or
#   - it has crashed MAX_RESTARTS times in a row (sends a failure email
#     and gives up).
#
# Usage (inside tmux):
#   ./edmingle_watchdog.sh --start-date 01-01-2010 --end-date 06-08-2026
#
# Rate limiting, retry delays, chunk size, and per-page size are now read
# from edmingle_config.json (not CLI flags) — edit that file to tune them.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_SCRIPT="edmingle_export.py"
CONFIG_FILE="edmingle_config.json"
MAX_RESTARTS=30
BACKOFF=10        # seconds; doubles after every crash, capped below
BACKOFF_CAP=300

ts() { date '+%Y-%m-%d %H:%M:%S'; }

attempt=0
while true; do
    attempt=$((attempt + 1))
    echo "$(ts) [watchdog] launch attempt $attempt: python3 $PYTHON_SCRIPT $*"
    python3 "$PYTHON_SCRIPT" "$@"
    exit_code=$?

    if [ "$exit_code" -eq 0 ]; then
        echo "$(ts) [watchdog] script finished successfully. exiting watchdog."
        exit 0
    fi

    echo "$(ts) [watchdog] script exited with code $exit_code (crash #$attempt)."

    if [ "$attempt" -ge "$MAX_RESTARTS" ]; then
        echo "$(ts) [watchdog] reached MAX_RESTARTS=$MAX_RESTARTS. giving up and sending failure email."
        python3 "$PYTHON_SCRIPT" --config "$CONFIG_FILE" --notify-failure --failure-attempts "$attempt"
        exit 1
    fi

    echo "$(ts) [watchdog] sleeping ${BACKOFF}s before restart..."
    sleep "$BACKOFF"
    BACKOFF=$(( BACKOFF * 2 ))
    if [ "$BACKOFF" -gt "$BACKOFF_CAP" ]; then
        BACKOFF=$BACKOFF_CAP
    fi
done
