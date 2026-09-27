#!/usr/bin/env bash
# Called by cron (deploy/crontab). Runs one daily batch in a fresh container.
#
# Why a wrapper script instead of putting the command straight in crontab:
# cron runs with a minimal environment (no PATH to docker, wrong working
# directory). The script pins both, and appends timestamped output to a log
# file so every scheduled run leaves evidence.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${REPO_DIR}/logs"
mkdir -p "$LOG_DIR"
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

cd "$REPO_DIR"
echo "=== $(date -Iseconds) cron run starting" >> "${LOG_DIR}/daily_run.log"
status=0
docker compose run --rm ingestion "$@" >> "${LOG_DIR}/daily_run.log" 2>&1 || status=$?
echo "=== $(date -Iseconds) cron run finished (exit ${status})" >> "${LOG_DIR}/daily_run.log"
exit "$status"
