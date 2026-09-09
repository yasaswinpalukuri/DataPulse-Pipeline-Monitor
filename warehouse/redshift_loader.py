"""
Loads mapped rows into Redshift. Two paths, on purpose:

1. load_taxi_trips_executemany() -- same shape as the Snowflake
   loader, fine for small/dev-scale batches (hundreds of rows).

2. load_taxi_trips_via_copy() -- the pattern you'd actually use in
   production at real volume. Redshift is a columnar MPP warehouse:
   its compute nodes are built for parallel bulk reads from S3, not
   for accepting many small row-by-row network writes from a client.
   COPY tells each compute slice to pull its portion of a staged S3
   file in parallel, which is orders of magnitude faster than
   executemany at scale -- and it's the answer interviewers are
   listening for when they ask "how do you load data into Redshift
   efficiently."

Both are kept so you can point at the executemany version to show you
understand the DB-API basics, and the COPY version to show you know
why that's NOT what you'd ship for a real pipeline into Redshift.
"""

import csv
import io

import boto3

from warehouse.redshift_client import get_connection

INSERT_TAXI_TRIPS_SQL = """
    INSERT INTO datapulse.raw.taxi_trips
        (trip_id, vendor_id, pickup_datetime, dropoff_datetime,
         passenger_count, trip_distance, fare_amount, tip_amount,
         payment_type, ingested_at, run_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

TAXI_TRIP_COLUMNS = [
    "trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime",
    "passenger_count", "trip_distance", "fare_amount", "tip_amount",
    "payment_type", "ingested_at", "run_id",
]


def load_taxi_trips_executemany(rows: list[dict]) -> int:
    """Small-batch path. redshift_connector's executemany still issues
    N individual INSERT statements under the hood (Redshift's
    protocol doesn't support true multi-row batching the way some
    drivers do) -- it's convenient, not fast. Fine for dev-scale
    batches; see load_taxi_trips_via_copy for anything larger.
    """
    if not rows:
        return 0

    params = [tuple(row[col] for col in TAXI_TRIP_COLUMNS) for row in rows]

    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(INSERT_TAXI_TRIPS_SQL, params)
        conn.commit()
        return len(rows)


def load_taxi_trips_via_copy(rows: list[dict], s3_bucket: str, s3_staging_prefix: str) -> int:
    """Production-shape path: stage rows as CSV in S3, then COPY into Redshift.

    Why staging in S3 first instead of streaming rows to Redshift
    directly: COPY is a server-side, parallelized bulk load -- each of
    the cluster's compute slices reads and loads its own portion of
    the staged file concurrently. That parallelism is only possible
    because the data already exists as a file Redshift's compute nodes
    can independently seek into; there's no equivalent "parallel bulk
    accept" for a client streaming rows over a single connection.
    """
    if not rows:
        return 0

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for row in rows:
        writer.writerow([row[col] for col in TAXI_TRIP_COLUMNS])

    s3_key = f"{s3_staging_prefix}/taxi_trips_{rows[0]['run_id']}.csv"
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=buffer.getvalue())

    copy_sql = f"""
        COPY datapulse.raw.taxi_trips ({", ".join(TAXI_TRIP_COLUMNS)})
        FROM 's3://{s3_bucket}/{s3_key}'
        IAM_ROLE default
        CSV
    """

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(copy_sql)
        conn.commit()
        return len(rows)
