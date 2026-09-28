"""
AWS Glue 5.0 job: one month of TLC Yellow Taxi Parquet -> star schema Parquet in S3.

Thin on purpose: all transformation logic lives in star_schema_core.py
(pure PySpark, unit-tested locally). This file only does what needs Glue:
read job arguments, read/write S3, and log counts to CloudWatch.

Idempotency: dimensions are small and fully rebuilt (mode=overwrite). The
fact is written with partitionOverwriteMode=dynamic, so rerunning a month
replaces only year=YYYY/month=M in S3 and leaves every other month alone --
the S3 equivalent of the delete-the-day/re-insert pattern on the Snowflake path.

Job arguments:
    --RAW_TRIPS_PATH    s3://.../raw/tlc/yellow/year=2023/month=01/
    --ZONE_LOOKUP_PATH  s3://.../raw/reference/taxi_zone_lookup.csv
    --CURATED_PATH      s3://.../curated/star/
    --YEAR / --MONTH    the month being processed
"""

import json
import sys
from datetime import date

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from transforms.star_schema_core import (
    build_dim_date,
    build_dim_payment_type,
    build_dim_rate_code,
    build_dim_time,
    build_dim_zone,
    build_fact_trips,
    clean_trips,
    normalize_source,
)

# dim_date covers the whole source year plus Jan 1 of the next year: a trip
# picked up on Dec 31 can be dropped off after midnight (max trip is 24h).
DIM_DATE_START = date(2023, 1, 1)
DIM_DATE_END = date(2024, 1, 1)


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "RAW_TRIPS_PATH", "ZONE_LOOKUP_PATH", "CURATED_PATH", "YEAR", "MONTH"],
    )
    year, month = int(args["YEAR"]), int(args["MONTH"])
    curated = args["CURATED_PATH"].rstrip("/")

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # TLC timestamps are NYC wall-clock times with no zone attached. Pinning
    # the session to UTC means Spark never shifts them, so 23:50 stays 23:50
    # and date/time keys are exactly what the meter recorded.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    raw = normalize_source(spark.read.parquet(args["RAW_TRIPS_PATH"]))
    zones = spark.read.option("header", True).csv(args["ZONE_LOOKUP_PATH"])

    trips, counts = clean_trips(raw, year, month)
    dim_zone = build_dim_zone(zones)
    valid_zone_keys = [r.zone_key for r in dim_zone.select("zone_key").collect()]

    dims = {
        "dim_date": build_dim_date(spark, DIM_DATE_START, DIM_DATE_END),
        "dim_time": build_dim_time(spark),
        "dim_zone": dim_zone,
        "dim_payment_type": build_dim_payment_type(spark),
        "dim_rate_code": build_dim_rate_code(spark),
    }
    for name, df in dims.items():
        # Dimensions are tiny: one file each keeps S3 tidy and COPY simple.
        df.coalesce(1).write.mode("overwrite").parquet(f"{curated}/{name}/")

    fact = build_fact_trips(trips, valid_zone_keys)
    (
        fact.repartition("year", "month")  # one file per month partition
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{curated}/fact_trips/")
    )

    # Printed to CloudWatch Logs: the run's evidence (rows in/out, exclusions).
    print("DATAPULSE_STAR_SCHEMA_COUNTS " + json.dumps({"year": year, "month": month, **counts}))
    job.commit()


if __name__ == "__main__":
    main()
