"""
S3 -> Snowflake raw: load TLC monthly Parquet from the S3 raw zone into
raw.tlc_yellow_trips with an external stage and COPY INTO.

    python -m ingestion.load_s3_to_snowflake

Runs in the daily cron job (scripts/run_daily.sh) between API ingestion and
dbt. With nothing new in S3 it's a few seconds and a no-op.

Design notes (likely interview questions):

- Storage integration, not AWS keys. Snowflake's own IAM user assumes a
  role that can only read s3://<bucket>/raw/tlc/*, and only with our
  integration's external ID (scripts/aws/04_snowflake_role.sh). No
  long-lived AWS credentials exist in Snowflake or in this code.

- Idempotent at the FILE level. COPY INTO keeps load metadata per file for
  64 days and skips files it has already loaded, so rerunning is safe --
  that's why FORCE is never set. (The API path is idempotent differently:
  delete-and-reinsert one logical day.) Caveat: if a file with the same
  name were replaced with different contents, COPY would treat it as new;
  TLC monthly files are immutable, so that doesn't happen here.

- Separate raw table. API pages (daily) and TLC files (monthly) differ in
  grain and column naming; the raw layer mirrors sources rather than
  merging them. Unification belongs in dbt, downstream.

- Explicit column mapping from $1 (the Parquet record). Parquet field names
  are case-sensitive in Snowflake and TLC files drift ('airport_fee' vs
  'Airport_fee' across years), so that column reads both spellings.

- Audit trail: every file load is recorded in Snowflake's COPY_HISTORY /
  LOAD_HISTORY. This loader deliberately does NOT write to
  raw.pipeline_runs: that table is the API batch's history, and the quality
  gate uses it as the row-count baseline -- a 3M-row monthly file there
  would make the next daily run's row-count check block.
"""

import os
import re
import sys

from dotenv import load_dotenv

from warehouse.snowflake_client import get_connection

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")

TABLE = "datapulse.raw.tlc_yellow_trips"
STAGE = "datapulse.raw.tlc_s3_stage"
INTEGRATION = "datapulse_s3"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS datapulse.raw.tlc_yellow_trips (
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP_NTZ,
    dropoff_datetime TIMESTAMP_NTZ,
    passenger_count INTEGER,
    trip_distance FLOAT,
    ratecode_id INTEGER,
    store_and_fwd_flag VARCHAR,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    source_file VARCHAR,        -- METADATA$FILENAME: which S3 object the row came from
    source_file_row INTEGER,    -- METADATA$FILE_ROW_NUMBER: row position within that file
    loaded_at TIMESTAMP_NTZ
)
"""

# Parquet field -> (column cast). Order must match CREATE_TABLE_SQL.
PARQUET_FIELDS = [
    ("$1:VendorID", "INTEGER"),
    ("$1:tpep_pickup_datetime", "TIMESTAMP_NTZ"),
    ("$1:tpep_dropoff_datetime", "TIMESTAMP_NTZ"),
    ("$1:passenger_count", "INTEGER"),
    ("$1:trip_distance", "FLOAT"),
    ("$1:RatecodeID", "INTEGER"),
    ("$1:store_and_fwd_flag", "VARCHAR"),
    ("$1:PULocationID", "INTEGER"),
    ("$1:DOLocationID", "INTEGER"),
    ("$1:payment_type", "INTEGER"),
    ("$1:fare_amount", "FLOAT"),
    ("$1:extra", "FLOAT"),
    ("$1:mta_tax", "FLOAT"),
    ("$1:tip_amount", "FLOAT"),
    ("$1:tolls_amount", "FLOAT"),
    ("$1:improvement_surcharge", "FLOAT"),
    ("$1:total_amount", "FLOAT"),
    ("$1:congestion_surcharge", "FLOAT"),
    ("COALESCE($1:airport_fee, $1:Airport_fee)", "FLOAT"),  # casing drifts across TLC years
]

COPY_SQL = (
    f"COPY INTO {TABLE} FROM (SELECT "
    + ", ".join(f"{expr}::{typ}" for expr, typ in PARQUET_FIELDS)
    + ", METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"
    + f" FROM @{STAGE}/yellow/)"
    + " PATTERN = '.*yellow_tripdata_.*[.]parquet'"
    + " ON_ERROR = ABORT_STATEMENT"
)


def bucket() -> str:
    value = os.environ.get("DATAPULSE_BUCKET", "")
    if not _BUCKET_RE.fullmatch(value):
        raise ValueError(f"DATAPULSE_BUCKET is missing or invalid: {value!r}")
    return value


def create_stage_sql(bucket_name: str) -> str:
    """Stage URL can't be a bind parameter; the bucket is regex-validated first."""
    return (
        f"CREATE STAGE IF NOT EXISTS {STAGE} "
        f"URL = 's3://{bucket_name}/raw/tlc/' "
        f"STORAGE_INTEGRATION = {INTEGRATION} "
        "FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)"
    )


def summarize(copy_rows: list[tuple]) -> tuple[int, int]:
    """(files_loaded, rows_loaded) from COPY INTO's result.

    COPY returns one row per file (file, status, rows_parsed, rows_loaded, ...),
    or a single 'Copy executed with 0 files processed.' row when every file
    was already loaded -- the idempotent no-op case.
    """
    files = rows = 0
    for row in copy_rows:
        if len(row) >= 4 and str(row[1]).upper() == "LOADED":
            files += 1
            rows += int(row[3])
    return files, rows


def main() -> int:
    load_dotenv()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(create_stage_sql(bucket()))
        cur.execute(COPY_SQL)
        results = cur.fetchall()
    files, rows = summarize(results)
    for row in results:
        print(" | ".join(str(v) for v in row[:4]))
    print(f"S3 -> Snowflake: {files} new file(s), {rows} row(s) loaded into {TABLE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
