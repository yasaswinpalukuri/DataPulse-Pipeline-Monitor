#!/usr/bin/env bash
# Create the DataPulse bucket (private, encrypted) and land one month of TLC
# data plus the zone lookup in the raw zone. Cost: pennies (~50 MB stored).
set -euo pipefail
source "$(dirname "$0")/env.sh"
YEAR="${1:-2023}"; MONTH="${2:-01}"

if ! aws s3api head-bucket --bucket "$DATAPULSE_BUCKET" 2>/dev/null; then
  aws s3api create-bucket --bucket "$DATAPULSE_BUCKET"   # us-east-1 needs no LocationConstraint
fi
# Block every form of public access -- this bucket is never meant to be public.
aws s3api put-public-access-block --bucket "$DATAPULSE_BUCKET" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket "$DATAPULSE_BUCKET" \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

TMP="$(mktemp -d)"
FILE="yellow_tripdata_${YEAR}-${MONTH}.parquet"
curl -fsSL "https://d37ci6vzurychx.cloudfront.net/trip-data/${FILE}" -o "$TMP/$FILE"
curl -fsSL "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" -o "$TMP/taxi_zone_lookup.csv"

# Hive-style partition path (year=/month=) so Glue, Spark and Athena can all
# prune by month without reading other files.
aws s3 cp "$TMP/$FILE" "s3://$DATAPULSE_BUCKET/raw/tlc/yellow/year=${YEAR}/month=${MONTH}/$FILE"
aws s3 cp "$TMP/taxi_zone_lookup.csv" "s3://$DATAPULSE_BUCKET/raw/reference/taxi_zone_lookup.csv"
rm -rf "$TMP"
aws s3 ls --recursive --human-readable "s3://$DATAPULSE_BUCKET/raw/"
