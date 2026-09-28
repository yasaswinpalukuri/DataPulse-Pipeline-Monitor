#!/usr/bin/env bash
# Shared settings for the AWS scripts. Usage: source scripts/aws/env.sh
#
# The bucket name embeds the account id: S3 names are globally unique, and
# this makes ours unique by construction (no "bucket already exists" guessing).
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export ACCOUNT_ID
export DATAPULSE_BUCKET="datapulse-${ACCOUNT_ID}-${AWS_REGION}"
export GLUE_ROLE="datapulse-glue-role"
export REDSHIFT_ROLE="datapulse-redshift-role"
export GLUE_JOB="datapulse-star-schema"
export REDSHIFT_WORKGROUP="datapulse"
export REDSHIFT_NAMESPACE="datapulse"
export REDSHIFT_DATABASE="dev"
echo "account=$ACCOUNT_ID region=$AWS_REGION bucket=$DATAPULSE_BUCKET"
