"""Concept tests: the star schema (real local Spark, no AWS)."""

import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from scripts.aws.load_redshift import dimension_sqls, fact_month_sqls, month_bounds
from transforms.star_schema_core import (
    UNKNOWN_KEY,
    build_dim_date,
    build_dim_payment_type,
    build_dim_rate_code,
    build_dim_time,
    build_dim_zone,
    build_fact_trips,
    clean_trips,
    normalize_source,
)

DDL = (Path(__file__).resolve().parents[2] / "redshift" / "ddl.sql").read_text()


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("star-schema-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session


def _trip(**overrides):
    base = {
        "VendorID": 2, "tpep_pickup_datetime": datetime(2023, 1, 15, 14, 30, 0),
        "tpep_dropoff_datetime": datetime(2023, 1, 15, 14, 52, 30), "passenger_count": 1.0,
        "trip_distance": 3.1, "RatecodeID": 1.0, "store_and_fwd_flag": "N",
        "PULocationID": 161, "DOLocationID": 132, "payment_type": 1, "fare_amount": 17.7,
        "extra": 0.0, "mta_tax": 0.5, "tip_amount": 3.0, "tolls_amount": 0.0,
        "improvement_surcharge": 1.0, "total_amount": 24.7, "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,  # capitalised on purpose: TLC files vary month to month
    }
    return {**base, **overrides}


@pytest.fixture(scope="module")
def zones(spark):
    return spark.createDataFrame(
        [(132, "Queens", "JFK Airport", "Airports"), (161, "Manhattan", "Midtown Center", "Yellow Zone")],
        ["LocationID", "Borough", "Zone", "service_zone"],
    )


def test_normalize_handles_column_case_and_type_drift(spark):
    """I learned: TLC monthly files drift ('Airport_fee' vs 'airport_fee',
    passenger_count as double vs long). Lower-casing and casting to a fixed
    contract makes every month produce the same schema."""
    df = normalize_source(spark.createDataFrame([_trip()]))
    row = df.first()
    assert "airport_fee" in df.columns and row.airport_fee == 0.0
    assert dict(df.dtypes)["passenger_count"] == "int"


def test_clean_keeps_refunds_but_drops_unplaceable_rows(spark):
    """I learned: the curated layer removes only rows that can't be placed in
    the model. A negative fare is a refund -- real data -- so it stays."""
    rows = [
        _trip(),
        _trip(fare_amount=-17.7, total_amount=-24.7),                          # refund: keep
        _trip(tpep_pickup_datetime=datetime(2008, 12, 31, 23, 0, 0),
              tpep_dropoff_datetime=datetime(2008, 12, 31, 23, 10, 0)),        # stray year
        _trip(tpep_dropoff_datetime=datetime(2023, 1, 15, 14, 0, 0)),          # ends before start
        _trip(tpep_dropoff_datetime=datetime(2023, 1, 17, 14, 30, 0)),         # 48-hour trip
    ]
    clean, counts = clean_trips(normalize_source(spark.createDataFrame(rows)), 2023, 1)
    assert clean.count() == 2
    assert counts == {"rows_in": 5, "missing_timestamps": 0, "outside_month": 1,
                      "bad_duration": 2, "rows_out": 2}


def test_dim_date_is_generated_calendar_with_smart_keys(spark):
    """I learned: generate the calendar instead of deriving it from trips, so
    a day with zero trips still exists. yyyymmdd keys are readable and
    range-filterable."""
    dim = build_dim_date(spark, date(2023, 1, 1), date(2024, 1, 1))
    assert dim.count() == 366
    jan2 = dim.filter("date_key = 20230102").first()   # 2023-01-02 was a Monday
    assert (jan2.day_name, jan2.day_of_week, jan2.is_weekend) == ("Monday", 1, False)
    assert dim.filter("date_key = 20230101").first().is_weekend  # Sunday


def test_dim_time_is_minute_grain(spark):
    """I learned: date (365) + time (1,440) replace a 525,600-row datetime dimension."""
    dim = build_dim_time(spark)
    assert dim.count() == 1440
    row = dim.filter("time_key = 1430").first()
    assert (row.hour, row.minute, row.time_label, row.day_part) == (14, 30, "14:30", "Afternoon")
    assert dim.filter("time_key = 800").first().is_morning_peak


def test_lookup_dimensions_include_an_unknown_member(spark, zones):
    """I learned: an explicit -1 'Unknown' row means every fact key resolves,
    even for codes missing from the dictionary."""
    for dim, key in [(build_dim_zone(zones), "zone_key"),
                     (build_dim_payment_type(spark), "payment_type_key"),
                     (build_dim_rate_code(spark), "rate_code_key")]:
        assert dim.filter(f"{key} = {UNKNOWN_KEY}").count() == 1


def test_fact_keys_role_play_and_conform_unknown_codes(spark):
    """I learned: the same zone dimension plays pickup AND dropoff; codes
    outside a dimension (payment_type 0, LocationID 999) map to -1 instead
    of being lost by an inner join."""
    rows = [_trip(), _trip(payment_type=0, DOLocationID=999)]
    trips = normalize_source(spark.createDataFrame(rows))
    fact = build_fact_trips(trips, valid_zone_keys=[132, 161, -1]).orderBy("payment_type_key")
    unknown, normal = fact.collect()
    assert (normal.pickup_date_key, normal.pickup_time_key) == (20230115, 1430)
    assert (normal.dropoff_time_key, normal.trip_duration_minutes) == (1452, 22.5)
    assert (normal.pu_zone_key, normal.do_zone_key) == (161, 132)
    assert (unknown.payment_type_key, unknown.do_zone_key) == (UNKNOWN_KEY, UNKNOWN_KEY)


def test_money_is_exact_decimal(spark):
    """I learned: sum(double) drifts (0.1 + 0.2 != 0.3). Revenue must be exact."""
    trips = normalize_source(spark.createDataFrame([_trip(total_amount=0.1), _trip(total_amount=0.2)]))
    fact = build_fact_trips(trips, [132, 161, -1])
    assert fact.agg({"total_amount": "sum"}).first()[0] == Decimal("0.30")


def test_trip_id_is_deterministic_across_reruns(spark):
    trips = normalize_source(spark.createDataFrame([_trip()]))
    first = build_fact_trips(trips, [132, 161, -1]).first().trip_id
    again = build_fact_trips(trips, [132, 161, -1]).first().trip_id
    assert first == again and len(first) == 32


def _ddl_columns(table: str) -> list[str]:
    body = re.search(rf"CREATE TABLE IF NOT EXISTS star\.{table} \((.*?)\)\s*DISTSTYLE", DDL, re.S)
    return [
        line.strip().split()[0]
        for line in body.group(1).splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]


def test_redshift_ddl_column_order_matches_parquet(spark, zones):
    """I learned: COPY ... FORMAT AS PARQUET maps columns by POSITION. If the
    DDL order drifts from the Spark output, fares load into the wrong
    columns with no error. This test is the guard. (year/month are S3
    partition folders, not columns inside the Parquet files.)"""
    trips = normalize_source(spark.createDataFrame([_trip()]))
    fact_cols = [c for c in build_fact_trips(trips, [132, 161, -1]).columns if c not in ("year", "month")]
    assert _ddl_columns("fact_trips") == fact_cols
    assert _ddl_columns("dim_date") == build_dim_date(spark, date(2023, 1, 1), date(2023, 1, 2)).columns
    assert _ddl_columns("dim_time") == build_dim_time(spark).columns
    assert _ddl_columns("dim_zone") == build_dim_zone(zones).columns
    assert _ddl_columns("dim_payment_type") == build_dim_payment_type(spark).columns
    assert _ddl_columns("dim_rate_code") == build_dim_rate_code(spark).columns


def test_ddl_has_exactly_one_fact_and_five_dimensions():
    """The resume says 1 fact + 5 dimension tables. The DDL is the proof."""
    tables = re.findall(r"CREATE TABLE IF NOT EXISTS star\.(\w+)", DDL)
    assert [t for t in tables if t.startswith("fact_")] == ["fact_trips"]
    assert len([t for t in tables if t.startswith("dim_")]) == 5


def test_month_load_is_one_overwrite_transaction():
    """I learned: stage -> delete month -> insert, all in one Data API batch
    (one transaction). DELETE, not TRUNCATE: TRUNCATE commits immediately."""
    sqls = dimension_sqls("datapulse-123-us-east-1") + fact_month_sqls("datapulse-123-us-east-1", 2023, 2)
    assert not any(s.upper().startswith("TRUNCATE") for s in sqls)
    assert "BETWEEN 20230201 AND 20230228" in sqls[-2]
    assert sqls[-1] == "INSERT INTO star.fact_trips SELECT * FROM fact_stage"
    assert month_bounds(2024, 2) == (20240201, 20240229)  # leap year
