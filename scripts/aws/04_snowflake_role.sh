#!/usr/bin/env bash
# IAM role that Snowflake's storage integration assumes to read raw/tlc/* in S3.
# Two phases, because the trust policy needs values Snowflake only generates
# after the integration exists:
#
#   bash scripts/aws/04_snowflake_role.sh
#       -> creates the role with a placeholder trust + read-only S3 policy,
#          prints the SQL to run in Snowflake (CREATE STORAGE INTEGRATION)
#   bash scripts/aws/04_snowflake_role.sh <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>
#       -> locks the trust policy to Snowflake's IAM user + your external ID
#
# Why an external ID: without it, any Snowflake customer who learned this
# role's ARN could point their own integration at it (the "confused deputy"
# problem). The external ID is unique to our integration.
set -euo pipefail
source "$(dirname "$0")/env.sh"
IAM_DIR="$(dirname "$0")/../../infra/iam"
ROLE="datapulse-snowflake-role"

if [ "$#" -eq 0 ]; then
  if ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
    # Placeholder trust: only our own account, with a dummy external ID.
    sed -e "s|__SNOWFLAKE_IAM_USER_ARN__|arn:aws:iam::${ACCOUNT_ID}:root|" \
        -e "s|__EXTERNAL_ID__|0000|" "$IAM_DIR/snowflake-trust.json" > /tmp/trust.json
    aws iam create-role --role-name "$ROLE" --assume-role-policy-document file:///tmp/trust.json >/dev/null
  fi
  sed "s/__BUCKET__/$DATAPULSE_BUCKET/g" "$IAM_DIR/snowflake-policy.json" > /tmp/policy.json
  aws iam put-role-policy --role-name "$ROLE" --policy-name "${ROLE}-policy" \
    --policy-document file:///tmp/policy.json
  ROLE_ARN="$(aws iam get-role --role-name "$ROLE" --query Role.Arn --output text)"
  cat <<SQL

Role ready: $ROLE_ARN
Now run this in a Snowflake worksheet (as ACCOUNTADMIN):

CREATE STORAGE INTEGRATION IF NOT EXISTS datapulse_s3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '$ROLE_ARN'
  STORAGE_ALLOWED_LOCATIONS = ('s3://$DATAPULSE_BUCKET/raw/tlc/');

DESC INTEGRATION datapulse_s3;

Then rerun this script with the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values:
  bash scripts/aws/04_snowflake_role.sh <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>
SQL
elif [ "$#" -eq 2 ]; then
  sed -e "s|__SNOWFLAKE_IAM_USER_ARN__|$1|" -e "s|__EXTERNAL_ID__|$2|" \
    "$IAM_DIR/snowflake-trust.json" > /tmp/trust.json
  aws iam update-assume-role-policy --role-name "$ROLE" --policy-document file:///tmp/trust.json
  echo "Trust policy locked to Snowflake's IAM user with your external ID."
else
  echo "usage: $0 [STORAGE_AWS_IAM_USER_ARN STORAGE_AWS_EXTERNAL_ID]" >&2; exit 2
fi
