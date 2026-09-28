#!/usr/bin/env bash
# Package the transform code, upload it, and create/update the Glue job.
# Creating the job is free; you pay only per run (~$0.44/DPU-hour, 1-min minimum).
set -euo pipefail
source "$(dirname "$0")/env.sh"
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD="$REPO/build"; mkdir -p "$BUILD"

# Glue imports 'transforms.star_schema_core' from this zip (--extra-py-files).
(cd "$REPO" && rm -f "$BUILD/transforms.zip" && zip -q "$BUILD/transforms.zip" transforms/__init__.py transforms/star_schema_core.py)
aws s3 cp "$REPO/transforms/glue_star_schema_job.py" "s3://$DATAPULSE_BUCKET/scripts/glue_star_schema_job.py"
aws s3 cp "$BUILD/transforms.zip" "s3://$DATAPULSE_BUCKET/scripts/transforms.zip"

ROLE_ARN="$(aws iam get-role --role-name "$GLUE_ROLE" --query Role.Arn --output text)"
JOB_SPEC=$(cat <<JSON
{
  "Role": "$ROLE_ARN",
  "Command": {"Name": "glueetl", "ScriptLocation": "s3://$DATAPULSE_BUCKET/scripts/glue_star_schema_job.py", "PythonVersion": "3"},
  "DefaultArguments": {"--extra-py-files": "s3://$DATAPULSE_BUCKET/scripts/transforms.zip", "--job-language": "python"},
  "GlueVersion": "5.0",
  "WorkerType": "G.1X",
  "NumberOfWorkers": 2,
  "Timeout": 30,
  "MaxRetries": 0
}
JSON
)
# 2 x G.1X is Glue's minimum; Timeout=30 min caps the worst-case bill of a hung run.
if aws glue get-job --job-name "$GLUE_JOB" >/dev/null 2>&1; then
  aws glue update-job --job-name "$GLUE_JOB" --job-update "$JOB_SPEC" >/dev/null
  echo "Updated Glue job $GLUE_JOB"
else
  aws glue create-job --name "$GLUE_JOB" --cli-input-json "$JOB_SPEC" >/dev/null
  echo "Created Glue job $GLUE_JOB"
fi
