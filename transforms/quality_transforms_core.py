"""
Core PySpark transformation logic for DataPulse.

Deliberately has ZERO dependency on awsglue.* imports. This is the
piece that gets unit-tested locally (see tests/learning/test_pyspark_
window_concepts.py, which actually runs a local Spark session against
this code). glue_run_quality_job.py is a thin wrapper around these
functions that only handles Glue-specific I/O (reading/writing
DynamicFrames, Glue job bookkeeping).

Why split it this way: AWS Glue's runtime (GlueContext, job
bookmarks, DynamicFrame) can't be instantiated outside an actual Glue
environment without heavy mocking. If the transformation logic lived
inside the Glue job script, none of it would be testable on a laptop.
Keeping the business logic in plain PySpark functions that take/return
DataFrames means it runs (and is tested) identically locally and in
Glue -- only the I/O layer differs.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def compute_rolling_anomaly_scores(
    df: DataFrame,
    value_col: str,
    partition_col: str,
    order_col: str,
    window_size: int = 10,
) -> DataFrame:
    """Flag rows whose value_col is >2 std devs from the ROLLING mean.

    Window: partitionBy(partition_col).orderBy(order_col).rowsBetween(-window_size, -1)

    The critical detail is `rowsBetween(-window_size, -1)`, NOT
    `(-window_size, 0)`. Ending the window at -1 (the row before the
    current one) excludes the current row from its own baseline
    mean/stddev. Including it (`0`) would mean an extreme value pulls
    its own comparison baseline toward itself, making genuine outliers
    look less anomalous the more extreme they are -- a classic
    look-ahead-bias bug in rolling anomaly detection that's a good
    thing to be able to spot and explain.

    Rows with fewer than 2 prior observations in their partition get
    NULL stddev (Spark's population stddev needs >=2 points) and are
    left un-flagged rather than raising -- there isn't enough history
    yet to judge them as anomalous or not.
    """
    window_spec = (
        Window.partitionBy(partition_col)
        .orderBy(order_col)
        .rowsBetween(-window_size, -1)
    )

    return (
        df.withColumn("rolling_mean", F.avg(value_col).over(window_spec))
        .withColumn("rolling_stddev", F.stddev(value_col).over(window_spec))
        .withColumn(
            "z_score",
            F.when(
                F.col("rolling_stddev").isNotNull() & (F.col("rolling_stddev") > 0),
                (F.col(value_col) - F.col("rolling_mean")) / F.col("rolling_stddev"),
            ),
        )
        .withColumn(
            "severity",
            F.when(F.col("z_score").isNull(), F.lit(None))
            .when(F.abs(F.col("z_score")) > 3, F.lit("high"))
            .when(F.abs(F.col("z_score")) > 2, F.lit("medium"))
            .otherwise(F.lit("low")),
        )
        .withColumn(
            "is_anomaly",
            F.when(F.abs(F.col("z_score")) > 2, True).otherwise(False),
        )
    )


def compute_quality_score_summary(df: DataFrame) -> DataFrame:
    """Aggregate per-check quality_results rows into one row per run.

    groupBy + agg here is Spark's equivalent of dbt's int_quality_scores
    model -- same aggregation, different engine. Worth being able to
    say explicitly which layer of the stack (SQL/dbt vs. Spark) you'd
    reach for and why: dbt wins for SQL-native, warehouse-pushdown
    aggregations; PySpark wins when the transformation needs
    row-level, imperative logic (like the rolling z-score above) that
    doesn't express cleanly as a single SQL window clause across
    engines, or when the data lives in S3/Parquet rather than already
    inside the warehouse.
    """
    return df.groupBy("run_id").agg(
        F.avg("score").alias("quality_score"),
        (F.sum(F.when(F.col("passed"), 1).otherwise(0)) / F.count("*")).alias(
            "pass_rate"
        ),
        F.count("*").alias("checks_run"),
        F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("checks_failed"),
    )


def compute_score_trend(df: DataFrame, run_id_col: str, started_at_col: str) -> DataFrame:
    """Attach a 'trend' column: quality_score vs. the PRECEDING run for the same run stream.

    Uses F.lag() over a window ordered by started_at -- a targeted,
    single-row lookback rather than the rolling window above, since
    "trend" here means "compared to the last run," not "compared to
    a 10-run baseline."
    """
    window_spec = Window.orderBy(started_at_col)

    return df.withColumn(
        "previous_quality_score", F.lag("quality_score").over(window_spec)
    ).withColumn(
        "trend",
        F.when(F.col("previous_quality_score").isNull(), F.lit("baseline"))
        .when(F.col("quality_score") > F.col("previous_quality_score"), F.lit("improving"))
        .when(F.col("quality_score") < F.col("previous_quality_score"), F.lit("declining"))
        .otherwise(F.lit("stable")),
    )
