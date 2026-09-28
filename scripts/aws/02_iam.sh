#!/usr/bin/env bash
# Two least-privilege roles: Glue (read raw + scripts, write curated, write
# its own logs) and Redshift (read curated/star only). Free.
set -euo pipefail
source "$(dirname "$0")/env.sh"
IAM_DIR="$(dirname "$0")/../../infra/iam"

create_role() {  # name, trust file, policy file
  local name="$1" trust="$2" policy="$3"
  if ! aws iam get-role --role-name "$name" >/dev/null 2>&1; then
    aws iam create-role --role-name "$name" --assume-role-policy-document "file://$trust" >/dev/null
  fi
  # Policies are templates: __BUCKET__ becomes the real bucket, so every
  # S3 permission is scoped to this one bucket and its prefixes.
  sed "s/__BUCKET__/$DATAPULSE_BUCKET/g" "$policy" > /tmp/policy.json
  aws iam put-role-policy --role-name "$name" --policy-name "${name}-policy" \
    --policy-document file:///tmp/policy.json
  aws iam get-role --role-name "$name" --query Role.Arn --output text
}

create_role "$GLUE_ROLE" "$IAM_DIR/glue-trust.json" "$IAM_DIR/glue-policy.json"
create_role "$REDSHIFT_ROLE" "$IAM_DIR/redshift-trust.json" "$IAM_DIR/redshift-policy.json"
