"""
Loads mapped rows into the Snowflake raw layer.

Uses cur.executemany() for batch inserts rather than looping
cur.execute() per row. executemany sends the batch as one request;
row-by-row execute() would mean one network round-trip per row -- for
a 1000-row NYC API page, that's 1 request vs. 1000. This is the same
N+1 problem people usually only think about for ORMs, but it applies
just as much to raw DB-API cursors.
"""

from warehouse.snowflake_client import get_connection

INSERT_TAXI_TRIPS_SQL = """
    INSERT INTO datapulse.raw.taxi_trips
        (trip_id, vendor_id, pickup_datetime, dropoff_datetime,
         passenger_count, trip_distance, fare_amount, tip_amount,
         payment_type, ingested_at, run_id)
    VALUES (%(trip_id)s, %(vendor_id)s, %(pickup_datetime)s, %(dropoff_datetime)s,
            %(passenger_count)s, %(trip_distance)s, %(fare_amount)s, %(tip_amount)s,
            %(payment_type)s, %(ingested_at)s, %(run_id)s)
"""

INSERT_PIPELINE_RUN_SQL = """
    INSERT INTO datapulse.raw.pipeline_runs
        (run_id, started_at, completed_at, status, source,
         rows_ingested, rows_failed, duration_seconds)
    VALUES (%(run_id)s, %(started_at)s, %(completed_at)s, %(status)s, %(source)s,
            %(rows_ingested)s, %(rows_failed)s, %(duration_seconds)s)
"""


def load_taxi_trips(rows: list[dict]) -> int:
    """Batch-insert mapped taxi trip rows. Returns count inserted.

    No-ops on an empty list rather than issuing a pointless connection
    open/close cycle -- small thing, but it's the kind of guard that
    matters once this runs on a 60-second poll loop indefinitely.
    """
    if not rows:
        return 0

    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(INSERT_TAXI_TRIPS_SQL, rows)
        return cur.rowcount


def log_pipeline_run(run_record: dict) -> None:
    """Insert a single row into pipeline_runs marking this run's outcome.

    Single execute(), not executemany() -- there's exactly one run
    record per run, so batching machinery would be overhead with no
    payoff. Matching the tool to the cardinality of the data matters
    as much as using the tool at all.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(INSERT_PIPELINE_RUN_SQL, run_record)
