-- DataPulse star schema in Redshift Serverless: 1 fact + 5 dimensions.
-- Loaded from the Glue job's Parquet output with COPY (scripts/aws/load_redshift.py).
--
-- Physical design (interview questions):
-- * Dimensions: DISTSTYLE ALL -- a full copy on every compute slice, so every
--   fact-to-dimension join is local with no data redistribution. Fine
--   because they're tiny (366, 1440, 266, 7, 8 rows).
-- * Fact: DISTSTYLE AUTO, not DISTKEY(pu_zone_key). Pickup zones are heavily
--   skewed (Midtown and the airports dominate), so a zone distkey would pile
--   rows onto a few slices. AUTO lets Redshift choose (EVEN at this size).
-- * Fact SORTKEY(pickup_date_key): most queries filter by date, so zone maps
--   let Redshift skip blocks outside the range.
-- * PRIMARY/FOREIGN KEYs are informational in Redshift (not enforced), but
--   the planner uses them. Integrity comes from the Unknown (-1) members.
-- * Column ORDER must match the Parquet files: COPY ... FORMAT AS PARQUET
--   maps columns by position. tests/learning/test_star_schema_concepts.py
--   checks this against the Spark output schema.

CREATE SCHEMA IF NOT EXISTS star;

CREATE TABLE IF NOT EXISTS star.dim_date (
    date_key INTEGER NOT NULL PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(16),
    day_of_month INTEGER,
    day_of_week INTEGER,          -- ISO: 1 = Monday ... 7 = Sunday
    day_name VARCHAR(16),
    is_weekend BOOLEAN
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS star.dim_time (
    time_key INTEGER NOT NULL PRIMARY KEY,   -- hhmm, e.g. 1430
    hour INTEGER,
    minute INTEGER,
    time_label VARCHAR(5),
    day_part VARCHAR(16),
    is_morning_peak BOOLEAN,
    is_evening_peak BOOLEAN
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS star.dim_zone (
    zone_key INTEGER NOT NULL PRIMARY KEY,   -- TLC LocationID; -1 = Unknown
    borough VARCHAR(64),
    zone_name VARCHAR(128),
    service_zone VARCHAR(64)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS star.dim_payment_type (
    payment_type_key INTEGER NOT NULL PRIMARY KEY,
    payment_type_desc VARCHAR(32) NOT NULL
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS star.dim_rate_code (
    rate_code_key INTEGER NOT NULL PRIMARY KEY,
    rate_code_desc VARCHAR(32) NOT NULL
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS star.fact_trips (
    trip_id VARCHAR(32) NOT NULL,
    vendor_id INTEGER,                                            -- degenerate dimension
    pickup_date_key INTEGER REFERENCES star.dim_date (date_key),
    pickup_time_key INTEGER REFERENCES star.dim_time (time_key),
    dropoff_date_key INTEGER REFERENCES star.dim_date (date_key),
    dropoff_time_key INTEGER REFERENCES star.dim_time (time_key),
    pu_zone_key INTEGER REFERENCES star.dim_zone (zone_key),
    do_zone_key INTEGER REFERENCES star.dim_zone (zone_key),
    payment_type_key INTEGER REFERENCES star.dim_payment_type (payment_type_key),
    rate_code_key INTEGER REFERENCES star.dim_rate_code (rate_code_key),
    store_and_fwd_flag VARCHAR(1),
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    trip_duration_minutes DOUBLE PRECISION,
    fare_amount DECIMAL(10, 2),
    extra DECIMAL(10, 2),
    mta_tax DECIMAL(10, 2),
    tip_amount DECIMAL(10, 2),
    tolls_amount DECIMAL(10, 2),
    improvement_surcharge DECIMAL(10, 2),
    congestion_surcharge DECIMAL(10, 2),
    airport_fee DECIMAL(10, 2),
    total_amount DECIMAL(10, 2)
) DISTSTYLE AUTO SORTKEY (pickup_date_key);
