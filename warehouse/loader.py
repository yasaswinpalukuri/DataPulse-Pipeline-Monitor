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

Why rows and quarantined rows are swapped in the SAME transaction: a rerun
of a day must leave both tables consistent -- never the new good rows next
to yesterday's quarantine for the same date.

Why an explicit temp-table DDL rather than auto_create_table: schema
inference from a DataFrame guesses types (an all-null column has no type at
all). The load path should never guess.
"""

from datetime import date

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from warehouse.snowflake_client import get_connection

STAGE_TABLE = "TAXI_TRIPS_STAGE"
QUARANTINE_STAGE_TABLE = "TAXI_TRIPS_QUARANTINE_STAGE"

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

_TIMESTAMP_COLS = {"pickup_datetime", "dropoff_datetime", "ingested_at"}
_PICKUP_DATE_EXPR = "TO_DATE(TRY_TO_TIMESTAMP_NTZ(pickup_datetime))"


def _create_stage_sql(stage: str, columns: dict[str, str]) -> str:
    cols = ", ".join(f"{col} {typ}" for col, typ in columns.items())
    return f"CREATE OR REPLACE TEMPORARY TABLE {stage} ({cols})"


def _insert_from_stage_sql(target: str, stage: str, columns: dict[str, str]) -> str:
    """INSERT ... SELECT with explicit timestamp casts and a derived pickup_date.

    Table and column names can't be bound parameters (placeholders only work
    for VALUES), so this SQL is built as a string -- which is why bandit
    flags it. It's safe because every identifier comes from the module
    constants below, never from input; the assert enforces that allowlist
    instead of just claiming it.
    """
    assert target in _ALLOWED_TABLES and stage in _ALLOWED_TABLES, "unknown table"  # nosec B101
    select = ", ".join(
        f"TRY_TO_TIMESTAMP_NTZ({c})" if c in _TIMESTAMP_COLS else c for c in columns
    )
    column_list = ", ".join(columns)
    # Identifiers are allowlisted above, so B608 is a false positive here.
    # The nosec must sit on the same line as the f-string, with nothing after it.
    return f"INSERT INTO {target} ({column_list}, pickup_date) SELECT {select}, {_PICKUP_DATE_EXPR} FROM {stage}"  # nosec B608


QUARANTINE_COLUMNS = {**STAGE_COLUMNS, "failed_checks": "VARCHAR"}

TRIPS_TABLE = "datapulse.raw.taxi_trips"
QUARANTINE_TABLE = "datapulse.raw.taxi_trips_quarantine"
DELETE_DAY_SQL = "DELETE FROM {table} WHERE pickup_date = %s"
_ALLOWED_TABLES = {TRIPS_TABLE, QUARANTINE_TABLE, STAGE_TABLE, QUARANTINE_STAGE_TABLE}

INSERT_QUALITY_RESULT_SQL = """
    INSERT INTO datapulse.raw.quality_results
        (id, run_id, check_name, passed, score, rows_checked, rows_failed,
         message, checked_at)
    VALUES (%(id)s, %(run_id)s, %(check_name)s, %(passed)s, %(score)s,
            %(rows_checked)s, %(rows_failed)s, %(message)s, %(checked_at)s)
"""

PREVIOUS_RUN_TOTAL_SQL = """
    SELECT rows_ingested + COALESCE(rows_quarantined, 0) AS total
    FROM datapulse.raw.pipeline_runs
    WHERE status = 'success'
    ORDER BY started_at DESC
    LIMIT 1
"""

INSERT_PIPELINE_RUN_SQL = """
    INSERT INTO datapulse.raw.pipeline_runs
        (run_id, logical_date, started_at, completed_at, status, source,
         rows_ingested, rows_quarantined, rows_failed, duration_seconds, error_message)
    VALUES (%(run_id)s, %(logical_date)s, %(started_at)s, %(completed_at)s, %(status)s,
            %(source)s, %(rows_ingested)s, %(rows_quarantined)s, %(rows_failed)s,
            %(duration_seconds)s, %(error_message)s)
"""


def rows_to_stage_frame(rows: list[dict], columns: dict[str, str] = STAGE_COLUMNS) -> pd.DataFrame:
    """Build the staged DataFrame with explicit, nullable dtypes.

    pandas' nullable 'Int64' keeps missing integers as <NA> instead of
    silently upcasting the whole column to float (1 -> 1.0).
    """
    df = pd.DataFrame(rows, columns=list(columns))
    for col, typ in columns.items():
        if typ == "INTEGER":
            df[col] = df[col].astype("Int64")
        elif typ == "FLOAT":
            df[col] = df[col].astype("float64")
    return df


def overwrite_day_partition(
    rows: list[dict], quarantined: list[dict], logical_date: date
) -> int:
    """Replace logical_date in raw.taxi_trips AND raw.taxi_trips_quarantine.

    Returns rows inserted into raw.taxi_trips. An empty batch (no good and no
    quarantined rows) does NOT wipe the day -- that's an upstream problem, not
    "zero trips", and the quality gate blocks it before we get here anyway.
    """
    if not rows and not quarantined:
        return 0

    day = logical_date.isoformat()
    with get_connection() as conn:
        cur = conn.cursor()
        # DDL auto-commits in Snowflake, so staging happens before BEGIN.
        cur.execute(_create_stage_sql(STAGE_TABLE, STAGE_COLUMNS))
        cur.execute(_create_stage_sql(QUARANTINE_STAGE_TABLE, QUARANTINE_COLUMNS))
        if rows:
            write_pandas(conn, rows_to_stage_frame(rows), STAGE_TABLE, quote_identifiers=False)
        if quarantined:
            write_pandas(
                conn, rows_to_stage_frame(quarantined, QUARANTINE_COLUMNS),
                QUARANTINE_STAGE_TABLE, quote_identifiers=False,
            )

        cur.execute("BEGIN")
        try:
            cur.execute(DELETE_DAY_SQL.format(table=TRIPS_TABLE), (day,))
            cur.execute(DELETE_DAY_SQL.format(table=QUARANTINE_TABLE), (day,))
            cur.execute(_insert_from_stage_sql(TRIPS_TABLE, STAGE_TABLE, STAGE_COLUMNS))
            inserted = cur.rowcount
            cur.execute(
                _insert_from_stage_sql(QUARANTINE_TABLE, QUARANTINE_STAGE_TABLE, QUARANTINE_COLUMNS)
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        return inserted


def previous_run_total() -> int | None:
    """Total rows (loaded + quarantined) of the last successful run, for the row-count check."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(PREVIOUS_RUN_TOTAL_SQL)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None


def log_quality_results(records: list[dict]) -> None:
    """One row per check per run -- the history behind the quality dashboard."""
    if not records:
        return
    with get_connection() as conn:
        conn.cursor().executemany(INSERT_QUALITY_RESULT_SQL, records)


def log_pipeline_run(run_record: dict) -> None:
    """One row per run in raw.pipeline_runs -- the source for pipeline-health metrics."""
    with get_connection() as conn:
        conn.cursor().execute(INSERT_PIPELINE_RUN_SQL, run_record)
