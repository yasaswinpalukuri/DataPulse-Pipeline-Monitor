#!/usr/bin/env bash
# Delete EVERYTHING DataPulse created in AWS. Safe to run more than once.
# After this, the account's only DataPulse-related cost is zero.
set -uo pipefail
source "$(dirname "$0")/env.sh"

echo "--- Glue job"
aws glue delete-job --job-name "$GLUE_JOB"

echo "--- Redshift Serverless (workgroup first, then namespace; no final snapshot)"
aws redshift-serverless delete-workgroup --workgroup-name "$REDSHIFT_WORKGROUP"
echo "waiting for workgroup deletion..."
while aws redshift-serverless get-workgroup --workgroup-name "$REDSHIFT_WORKGROUP" >/dev/null 2>&1; do sleep 15; done
aws redshift-serverless delete-namespace --namespace-name "$REDSHIFT_NAMESPACE"

echo "--- IAM roles (inline policies must go before the role)"
for role in "$GLUE_ROLE" "$REDSHIFT_ROLE" datapulse-snowflake-role; do
  aws iam delete-role-policy --role-name "$role" --policy-name "${role}-policy"
  aws iam delete-role --role-name "$role"
done

echo "--- S3 bucket (empty it, then delete)"
aws s3 rm "s3://$DATAPULSE_BUCKET" --recursive
aws s3api delete-bucket --bucket "$DATAPULSE_BUCKET"

echo "--- Glue job logs"
for g in /aws-glue/jobs/output /aws-glue/jobs/error /aws-glue/jobs/logs-v2; do
  aws logs delete-log-group --log-group-name "$g" 2>/dev/null
done
echo "Snowflake side (run in a worksheet): DROP STAGE IF EXISTS datapulse.raw.tlc_s3_stage; DROP INTEGRATION IF EXISTS datapulse_s3;"
echo "Teardown complete. Check Billing > Bills over the next day to confirm."
