"""
Loads one logical day into Snowflake raw with PARTITION OVERWRITE semantics.

Why partition overwrite (delete the day, re-insert it) instead of MERGE:
trip_id is a content hash, so a corrected source row gets a NEW id. MERGE on
trip_id would insert the corrected row and keep the stale one. Deleting the
whole logical day and re-inserting it makes any rerun of a date converge to
exactly what the source says now -- idempotent for identical reloads AND
correct for corrections.

Why the delete and insert share one transaction: if the insert failed after
a committed delete, that day would silently vanish from raw. BEGIN ... COMMIT
(ROLLBACK on error) makes the swap all-or-nothing.

Why write_pandas into a temp table first: write_pandas uploads the DataFrame
as compressed Parquet to a Snowflake internal stage (PUT) and runs COPY INTO.
That's Snowflake's bulk-load path -- the same "stage a file, then COPY"
pattern as Redshift COPY FROM S3 -- instead of building a giant multi-row
INSERT on the client. The temp table is session-scoped and disappears when
the connection closes, so there's nothing to clean up.

Why an explicit temp-table DDL rather than auto_create_table: schema
inference from a DataFrame guesses types (an all-null column has no type at
all). The load path should never guess.
"""

from datetime import date

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from warehouse.snowflake_client import get_connection

STAGE_TABLE = "TAXI_TRIPS_STAGE"

# Column order for the staged DataFrame. Timestamps stay strings here and are
# converted explicitly in SQL (TRY_TO_TIMESTAMP_NTZ) below.
STAGE_COLUMNS = {
    "trip_id": "VARCHAR",
    "vendor_id": "INTEGER",
    "pickup_datetime": "VARCHAR",
    "dropoff_datetime": "VARCHAR",
    "passenger_count": "INTEGER",
    "trip_distance": "FLOAT",
    "ratecode_id": "INTEGER",
    "store_and_fwd_flag": "VARCHAR",
    "pu_location_id": "INTEGER",
    "do_location_id": "INTEGER",
    "payment_type": "INTEGER",
    "fare_amount": "FLOAT",
    "extra": "FLOAT",
    "mta_tax": "FLOAT",
    "tip_amount": "FLOAT",
    "tolls_amount": "FLOAT",
    "improvement_surcharge": "FLOAT",
    "total_amount": "FLOAT",
    "congestion_surcharge": "FLOAT",
    "airport_fee": "FLOAT",
    "ingested_at": "VARCHAR",
    "run_id": "VARCHAR",
}

CREATE_STAGE_SQL = (
    f"CREATE OR REPLACE TEMPORARY TABLE {STAGE_TABLE} ("
    + ", ".join(f"{col} {typ}" for col, typ in STAGE_COLUMNS.items())
    + ")"
)

DELETE_DAY_SQL = "DELETE FROM datapulse.raw.taxi_trips WHERE pickup_date = %s"

_TIMESTAMP_COLS = {"pickup_datetime", "dropoff_datetime", "ingested_at"}
INSERT_FROM_STAGE_SQL = (
    "INSERT INTO datapulse.raw.taxi_trips ("
    + ", ".join(STAGE_COLUMNS)
    + ", pickup_date) SELECT "
    + ", ".join(
        f"TRY_TO_TIMESTAMP_NTZ({c})" if c in _TIMESTAMP_COLS else c for c in STAGE_COLUMNS
    )
    + f", TO_DATE(TRY_TO_TIMESTAMP_NTZ(pickup_datetime)) FROM {STAGE_TABLE}"
)

INSERT_PIPELINE_RUN_SQL = """
    INSERT INTO datapulse.raw.pipeline_runs
        (run_id, logical_date, started_at, completed_at, status, source,
         rows_ingested, rows_failed, duration_seconds, error_message)
    VALUES (%(run_id)s, %(logical_date)s, %(started_at)s, %(completed_at)s, %(status)s,
            %(source)s, %(rows_ingested)s, %(rows_failed)s, %(duration_seconds)s,
            %(error_message)s)
"""


def rows_to_stage_frame(rows: list[dict]) -> pd.DataFrame:
    """Build the staged DataFrame with explicit, nullable dtypes.

    pandas' nullable 'Int64' keeps missing integers as <NA> instead of
    silently upcasting the whole column to float (1 -> 1.0).
    """
    df = pd.DataFrame(rows, columns=list(STAGE_COLUMNS))
    for col, typ in STAGE_COLUMNS.items():
        if typ == "INTEGER":
            df[col] = df[col].astype("Int64")
        elif typ == "FLOAT":
            df[col] = df[col].astype("float64")
    return df


def overwrite_day_partition(rows: list[dict], logical_date: date) -> int:
    """Replace raw.taxi_trips rows for logical_date with `rows`. Returns rows inserted.

    An empty batch does NOT wipe the day: an empty response for a valid 2023
    date is far more likely an upstream problem than a real "zero trips" day,
    so we keep the last good data and let the run be marked 'partial'.
    """
    if not rows:
        return 0

    df = rows_to_stage_frame(rows)

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(CREATE_STAGE_SQL)  # DDL auto-commits, so it runs before BEGIN
        write_pandas(conn, df, STAGE_TABLE, quote_identifiers=False)

        cur.execute("BEGIN")
        try:
            cur.execute(DELETE_DAY_SQL, (logical_date.isoformat(),))
            cur.execute(INSERT_FROM_STAGE_SQL)
            inserted = cur.rowcount
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        return inserted


def log_pipeline_run(run_record: dict) -> None:
    """One row per run in raw.pipeline_runs -- the source for pipeline-health metrics."""
    with get_connection() as conn:
        conn.cursor().execute(INSERT_PIPELINE_RUN_SQL, run_record)
