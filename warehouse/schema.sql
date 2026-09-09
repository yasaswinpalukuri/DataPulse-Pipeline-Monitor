-- DataPulse raw layer schema.
-- Idempotent (IF NOT EXISTS) so this can be re-run safely in CI or by a
-- new contributor setting up their own Snowflake trial account.

CREATE DATABASE IF NOT EXISTS datapulse;

CREATE SCHEMA IF NOT EXISTS datapulse.raw;
CREATE SCHEMA IF NOT EXISTS datapulse.staging;
CREATE SCHEMA IF NOT EXISTS datapulse.marts;

CREATE TABLE IF NOT EXISTS datapulse.raw.taxi_trips (
    trip_id VARCHAR,
    vendor_id VARCHAR,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    payment_type VARCHAR,
    ingested_at TIMESTAMP,
    run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS datapulse.raw.pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR,
    source VARCHAR,
    rows_ingested INTEGER,
    rows_failed INTEGER,
    duration_seconds FLOAT
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
