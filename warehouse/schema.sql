-- DataPulse raw layer schema.
-- Idempotent (IF NOT EXISTS) so this can be re-run safely in CI or by a
-- new contributor setting up their own Snowflake trial account.

CREATE DATABASE IF NOT EXISTS datapulse;

CREATE SCHEMA IF NOT EXISTS datapulse.raw;
CREATE SCHEMA IF NOT EXISTS datapulse.staging;
CREATE SCHEMA IF NOT EXISTS datapulse.marts;

CREATE TABLE IF NOT EXISTS datapulse.raw.taxi_trips (
    trip_id VARCHAR,               -- sha256 of the full source record (see nyc_api_reader)
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
    ingested_at TIMESTAMP_NTZ,
    run_id VARCHAR,
    pickup_date DATE               -- partition key for delete+insert overwrite
);
-- No CLUSTER BY on purpose: each daily load inserts one day in one go, so
-- micro-partitions are already naturally grouped by pickup_date. An explicit
-- clustering key turns on Automatic Clustering, a background service that
-- bills credits -- a cost with no benefit at this data volume.

-- Rows that failed at least one row-level quality check (see quality/gate.py).
-- Same shape as taxi_trips plus the names of the checks each row failed.
CREATE TABLE IF NOT EXISTS datapulse.raw.taxi_trips_quarantine (
    trip_id VARCHAR,
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
    ingested_at TIMESTAMP_NTZ,
    run_id VARCHAR,
    failed_checks VARCHAR,
    pickup_date DATE
);

CREATE TABLE IF NOT EXISTS datapulse.raw.pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    logical_date DATE,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR,
    source VARCHAR,
    rows_ingested INTEGER,         -- rows loaded into raw.taxi_trips
    rows_quarantined INTEGER,      -- rows that failed a row-level quality check
    rows_failed INTEGER,           -- source records skipped at mapping (no identity)
    duration_seconds FLOAT,
    error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS datapulse.raw.quality_results (
    id VARCHAR,
    run_id VARCHAR,
    check_name VARCHAR,
    passed BOOLEAN,
    score FLOAT,
    rows_checked INTEGER,
    rows_failed INTEGER,
    message VARCHAR,
    checked_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS datapulse.raw.anomalies (
    id VARCHAR,
    run_id VARCHAR,
    column_name VARCHAR,
    anomaly_type VARCHAR,
    severity VARCHAR,
    expected_range VARCHAR,
    actual_value FLOAT,
    detected_at TIMESTAMP
);
