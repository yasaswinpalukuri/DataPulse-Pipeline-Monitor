#!/usr/bin/env bash
# Called by cron (deploy/crontab). One daily batch in fresh containers:
# API -> Snowflake raw, S3 -> Snowflake raw (COPY INTO), then dbt.
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
log="${LOG_DIR}/daily_run.log"
status=0
docker compose run --rm ingestion "$@" >> "$log" 2>&1 || status=$?

# S3 raw zone -> Snowflake raw. COPY INTO skips files it already loaded, so
# this is a few-second no-op until a new monthly TLC file lands in S3.
echo "--- $(date -Iseconds) S3 -> Snowflake COPY" >> "$log"
docker compose run --rm --entrypoint python ingestion -m ingestion.load_s3_to_snowflake >> "$log" 2>&1 \
  || status=$((status > 0 ? status : 3))

# dbt runs even if ingestion was blocked or failed: the pipeline-health marts
# must reflect a bad run, not hide it. `dbt build` = seed + run + test in
# dependency order. Freshness warns if runs have stopped arriving.
echo "--- $(date -Iseconds) dbt build" >> "$log"
docker compose run --rm dbt build >> "$log" 2>&1 || status=$((status > 0 ? status : 2))
docker compose run --rm dbt source freshness >> "$log" 2>&1 || true

echo "=== $(date -Iseconds) cron run finished (exit ${status})" >> "$log"
exit "$status"
