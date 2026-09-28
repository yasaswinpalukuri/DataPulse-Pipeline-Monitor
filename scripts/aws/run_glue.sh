#!/usr/bin/env bash
# Run the star-schema job for one month and wait for it. Usage: run_glue.sh 2023 1
set -euo pipefail
source "$(dirname "$0")/env.sh"
YEAR="${1:-2023}"; MONTH="${2:-1}"; MM="$(printf '%02d' "$MONTH")"

RUN_ID=$(aws glue start-job-run --job-name "$GLUE_JOB" --query JobRunId --output text --arguments "{
  \"--RAW_TRIPS_PATH\": \"s3://$DATAPULSE_BUCKET/raw/tlc/yellow/year=$YEAR/month=$MM/\",
  \"--ZONE_LOOKUP_PATH\": \"s3://$DATAPULSE_BUCKET/raw/reference/taxi_zone_lookup.csv\",
  \"--CURATED_PATH\": \"s3://$DATAPULSE_BUCKET/curated/star/\",
  \"--YEAR\": \"$YEAR\", \"--MONTH\": \"$MONTH\"}")
echo "Started $RUN_ID"
while true; do
  STATE=$(aws glue get-job-run --job-name "$GLUE_JOB" --run-id "$RUN_ID" --query JobRun.JobRunState --output text)
  echo "$(date +%T) $STATE"
  case "$STATE" in
    SUCCEEDED) break ;;
    FAILED|ERROR|TIMEOUT|STOPPED)
      aws glue get-job-run --job-name "$GLUE_JOB" --run-id "$RUN_ID" --query JobRun.ErrorMessage --output text
      exit 1 ;;
  esac
  sleep 20
done
aws glue get-job-run --job-name "$GLUE_JOB" --run-id "$RUN_ID" \
  --query 'JobRun.{seconds:ExecutionTime,dpu_seconds:DPUSeconds}' --output table
aws s3 ls --recursive --human-readable "s3://$DATAPULSE_BUCKET/curated/star/"
