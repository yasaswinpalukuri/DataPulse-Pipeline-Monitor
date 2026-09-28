#!/usr/bin/env bash
# Post-deploy smoke test: call every API endpoint against the REAL warehouse.
# Unit tests use a fake repository, so they can't catch Snowflake SQL errors;
# this can. Usage: scripts/smoke_api.sh http://100.112.171.54:8010
set -uo pipefail
BASE="${1:-http://127.0.0.1:8010}"
fail=0
for path in /health /health/ready /pipeline/status "/pipeline/runs?limit=5" \
            "/pipeline/health/daily?days=7" "/quality/checks?runs=3" "/metrics/trips/daily?limit=5"; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE$path")
  printf '%-32s %s\n' "$path" "$code"
  [ "$code" = "200" ] || fail=1
done
[ "$fail" = 0 ] && echo "SMOKE TEST PASSED" || { echo "SMOKE TEST FAILED"; exit 1; }
