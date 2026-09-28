"""
Star schema for NYC Yellow Taxi trips: 1 fact + 5 dimensions.

Pure PySpark, no awsglue imports -- so every function here runs under local
pytest. transforms/glue_star_schema_job.py is a thin wrapper that only reads
arguments, calls these functions and writes to S3.

    fact_trips          grain: one row per trip
    dim_date            role-playing: pickup_date_key, dropoff_date_key
    dim_time            role-playing: pickup_time_key, dropoff_time_key (minute grain)
    dim_zone            role-playing: pu_zone_key, do_zone_key (TLC 265-zone lookup)
    dim_payment_type
    dim_rate_code
    vendor_id           degenerate dimension on the fact (2-3 values, one attribute;
                        a dimension table would add a join and no information)

Key design choices (likely interview questions):

- Smart keys for date (yyyymmdd) and time (hhmm). Kimball recommends a
  meaningful integer date key: readable, stable across reloads, and range
  filters (BETWEEN 20230101 AND 20230131) work directly on the fact.
  Zone/payment/rate-code keys are TLC's own stable codes.
- Separate date and time dimensions. A combined datetime dimension at
  minute grain would be 525,600 rows per year; date (365) + time (1,440)
  covers the same questions with tiny tables.
- An explicit Unknown member (-1) in every lookup dimension. A code that
  isn't in the dictionary (payment_type 0, a stray LocationID) maps to -1
  instead of being dropped by an inner join or leaving a dangling key.
- The curated layer only removes rows that cannot be placed in the model:
  pickup outside the file's month (TLC files contain stray years), missing
  timestamps, dropoff before pickup, or trips over 24 hours. Everything
  else -- including negative fares, which are refunds -- is real analytical
  data and stays. Row-level business rules are enforced on the Snowflake
  path by the Great Expectations gate; this path models, it doesn't gate.
"""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

UNKNOWN_KEY = -1
MONEY_TYPE = T.DecimalType(10, 2)
MAX_TRIP_HOURS = 24

# TLC Yellow Taxi data dictionary. payment_type 0 appears in 2023 data but
# isn't in the classic 1-6 list, so it maps to Unknown (-1) until verified.
PAYMENT_TYPES = {
    1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip",
}
RATE_CODES = {
    1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Nassau or Westchester",
    5: "Negotiated fare", 6: "Group ride", 99: "Null/unknown",
}

# Explicit source contract. Monthly TLC files drift: 'airport_fee' is
# 'Airport_fee' in some months and passenger_count is sometimes double,
# sometimes long. Lower-casing names and casting to fixed types makes every
# month produce the same schema.
SOURCE_TYPES = {
    "vendorid": T.IntegerType(),
    "tpep_pickup_datetime": T.TimestampType(),
    "tpep_dropoff_datetime": T.TimestampType(),
    "passenger_count": T.IntegerType(),
    "trip_distance": T.DoubleType(),
    "ratecodeid": T.IntegerType(),
    "store_and_fwd_flag": T.StringType(),
    "pulocationid": T.IntegerType(),
    "dolocationid": T.IntegerType(),
    "payment_type": T.IntegerType(),
    "fare_amount": T.DoubleType(),
    "extra": T.DoubleType(),
    "mta_tax": T.DoubleType(),
    "tip_amount": T.DoubleType(),
    "tolls_amount": T.DoubleType(),
    "improvement_surcharge": T.DoubleType(),
    "total_amount": T.DoubleType(),
    "congestion_surcharge": T.DoubleType(),
    "airport_fee": T.DoubleType(),
}

MONEY_COLUMNS = [
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "airport_fee", "total_amount",
]


# --------------------------------------------------------------------------
# Source preparation
# --------------------------------------------------------------------------
def normalize_source(df: DataFrame) -> DataFrame:
    """Lower-case column names and cast to SOURCE_TYPES. Missing columns become typed nulls."""
    df = df.toDF(*[c.lower() for c in df.columns])
    return df.select(
        *[
            (F.col(name) if name in df.columns else F.lit(None)).cast(dtype).alias(name)
            for name, dtype in SOURCE_TYPES.items()
        ]
    )


def clean_trips(df: DataFrame, year: int, month: int) -> tuple[DataFrame, dict[str, int]]:
    """Keep only rows that can be placed in the model. Returns (clean_df, counts).

    Counts are computed with one aggregation pass over flag columns rather
    than one count() per rule, so Spark scans the data once.
    """
    duration_hours = (
        F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")
    ) / 3600.0
    flagged = df.withColumns(
        {
            "_missing_ts": F.col("tpep_pickup_datetime").isNull()
            | F.col("tpep_dropoff_datetime").isNull(),
            "_wrong_month": (F.year("tpep_pickup_datetime") != year)
            | (F.month("tpep_pickup_datetime") != month),
            "_bad_duration": (duration_hours < 0) | (duration_hours > MAX_TRIP_HOURS),
        }
    )
    # A null flag (from a null timestamp) must count as "excluded", not "kept".
    for flag in ("_wrong_month", "_bad_duration"):
        flagged = flagged.withColumn(flag, F.coalesce(F.col(flag), F.lit(True)))

    totals = flagged.agg(
        F.count(F.lit(1)).alias("rows_in"),
        F.sum(F.col("_missing_ts").cast("int")).alias("missing_timestamps"),
        F.sum((~F.col("_missing_ts") & F.col("_wrong_month")).cast("int")).alias("outside_month"),
        F.sum(
            (~F.col("_missing_ts") & ~F.col("_wrong_month") & F.col("_bad_duration")).cast("int")
        ).alias("bad_duration"),
    ).first()
    counts = {k: int(v or 0) for k, v in totals.asDict().items()}

    clean = flagged.filter(
        ~F.col("_missing_ts") & ~F.col("_wrong_month") & ~F.col("_bad_duration")
    ).drop("_missing_ts", "_wrong_month", "_bad_duration")
    counts["rows_out"] = counts["rows_in"] - (
        counts["missing_timestamps"] + counts["outside_month"] + counts["bad_duration"]
    )
    return clean, counts


# --------------------------------------------------------------------------
# Dimensions
# --------------------------------------------------------------------------
def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """One row per calendar day in [start, end], generated -- not derived from trips.

    Generating the calendar means days with zero trips still exist, so
    "trips per day" reports show a 0 instead of silently skipping the day.
    """
    days = spark.sql(
        f"SELECT explode(sequence(to_date('{start.isoformat()}'), "
        f"to_date('{end.isoformat()}'), interval 1 day)) AS full_date"
    )
    return days.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        # dayofweek(): 1=Sunday..7=Saturday. Convert to ISO 1=Monday..7=Sunday.
        (((F.dayofweek("full_date") + 5) % 7) + 1).alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_time(spark: SparkSession) -> DataFrame:
    """1,440 rows: one per minute of the day. Minute grain is enough for taxi analysis."""
    minutes = spark.range(0, 24 * 60).select(
        (F.col("id") / 60).cast("int").alias("hour"),
        (F.col("id") % 60).cast("int").alias("minute"),
    )
    day_part = (
        F.when(F.col("hour") < 6, "Night")
        .when(F.col("hour") < 12, "Morning")
        .when(F.col("hour") < 17, "Afternoon")
        .when(F.col("hour") < 21, "Evening")
        .otherwise("Late night")
    )
    return minutes.select(
        (F.col("hour") * 100 + F.col("minute")).alias("time_key"),
        "hour",
        "minute",
        F.format_string("%02d:%02d", "hour", "minute").alias("time_label"),
        day_part.alias("day_part"),
        F.col("hour").between(7, 9).alias("is_morning_peak"),
        F.col("hour").between(16, 19).alias("is_evening_peak"),
    )


def _unknown_row(spark: SparkSession, schema: T.StructType, values: tuple) -> DataFrame:
    return spark.createDataFrame([values], schema)


def build_dim_zone(zone_lookup: DataFrame) -> DataFrame:
    """TLC 265-zone lookup plus an Unknown (-1) member. Used twice by the fact (pickup, dropoff)."""
    zl = zone_lookup.toDF(*[c.lower() for c in zone_lookup.columns])
    zones = zl.select(
        F.col("locationid").cast("int").alias("zone_key"),
        F.col("borough").cast("string").alias("borough"),
        F.col("zone").cast("string").alias("zone_name"),
        F.col("service_zone").cast("string").alias("service_zone"),
    )
    unknown = _unknown_row(
        zones.sparkSession, zones.schema, (UNKNOWN_KEY, "Unknown", "Unknown", "Unknown")
    )
    return zones.unionByName(unknown)


def _code_dimension(
    spark: SparkSession, codes: dict[int, str], key: str, desc: str
) -> DataFrame:
    schema = T.StructType(
        [T.StructField(key, T.IntegerType(), False), T.StructField(desc, T.StringType(), False)]
    )
    rows = [(code, label) for code, label in sorted(codes.items())] + [(UNKNOWN_KEY, "Unknown")]
    return spark.createDataFrame(rows, schema)


def build_dim_payment_type(spark: SparkSession) -> DataFrame:
    return _code_dimension(spark, PAYMENT_TYPES, "payment_type_key", "payment_type_desc")


def build_dim_rate_code(spark: SparkSession) -> DataFrame:
    return _code_dimension(spark, RATE_CODES, "rate_code_key", "rate_code_desc")


# --------------------------------------------------------------------------
# Fact
# --------------------------------------------------------------------------
def _date_key(ts: str):
    return F.date_format(ts, "yyyyMMdd").cast("int")


def _time_key(ts: str):
    return (F.hour(ts) * 100 + F.minute(ts)).cast("int")


def _conform(code_col: str, valid_codes) -> F.Column:
    """Map a code to itself if it's in the dimension, else to the Unknown member."""
    return (
        F.when(F.col(code_col).isin(list(valid_codes)), F.col(code_col))
        .otherwise(F.lit(UNKNOWN_KEY))
        .cast("int")
    )


def build_fact_trips(trips: DataFrame, valid_zone_keys: list[int]) -> DataFrame:
    """One row per trip. Foreign keys always resolve: unmatched codes -> Unknown (-1).

    trip_id is a sha256 of the full normalized source record, the same idea
    as the Snowflake path: deterministic, so a rerun of the same month
    produces identical ids.
    """
    hash_input = F.concat_ws(
        "|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SOURCE_TYPES]
    )
    duration_min = (
        F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")
    ) / 60.0

    return trips.select(
        F.substring(F.sha2(hash_input, 256), 1, 32).alias("trip_id"),
        F.col("vendorid").alias("vendor_id"),
        _date_key("tpep_pickup_datetime").alias("pickup_date_key"),
        _time_key("tpep_pickup_datetime").alias("pickup_time_key"),
        _date_key("tpep_dropoff_datetime").alias("dropoff_date_key"),
        _time_key("tpep_dropoff_datetime").alias("dropoff_time_key"),
        _conform("pulocationid", valid_zone_keys).alias("pu_zone_key"),
        _conform("dolocationid", valid_zone_keys).alias("do_zone_key"),
        _conform("payment_type", PAYMENT_TYPES).alias("payment_type_key"),
        _conform("ratecodeid", RATE_CODES).alias("rate_code_key"),
        "store_and_fwd_flag",
        "passenger_count",
        "trip_distance",
        F.round(duration_min, 2).alias("trip_duration_minutes"),
        # Money as DECIMAL, not double: sums of doubles drift (0.1 + 0.2 !=
        # 0.3), and revenue totals must be exact. The TLC files store fares
        # as doubles, so the cast happens here, once, at the modelled layer.
        *[F.col(c).cast(MONEY_TYPE).alias(c) for c in MONEY_COLUMNS],
        # Partition columns for S3 layout; not stored inside the Parquet files.
        F.year("tpep_pickup_datetime").alias("year"),
        F.month("tpep_pickup_datetime").alias("month"),
    )
