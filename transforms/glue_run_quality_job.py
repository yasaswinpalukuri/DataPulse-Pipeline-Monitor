"""
AWS Glue job: reads raw quality/pipeline data, applies the core PySpark
transforms (rolling z-score anomalies, quality score aggregation), and
writes results to Redshift.

This file is intentionally thin. All the actual transformation logic
lives in transforms/quality_transforms_core.py, which has zero AWS
dependencies and is unit-tested locally with a real Spark session (see
tests/learning/test_pyspark_window_concepts.py). This file's only job
is Glue-specific I/O: reading DynamicFrames, Glue job bookkeeping
(commit at the end so bookmarks advance), and writing to Redshift via
a Glue connection.

This script is meant to run INSIDE AWS Glue's managed Spark
environment (glueContext, getResolvedOptions, and Job are provided by
that runtime) -- it is not meant to run standalone, and importing it
outside Glue will fail at the `from awsglue...` imports. That's by
design: it should never be mistaken for something you can `python
glue_run_quality_job.py` locally. Local testing happens against
quality_transforms_core.py directly instead.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from transforms.quality_transforms_core import (
    compute_quality_score_summary,
    compute_rolling_anomaly_scores,
    compute_score_trend,
)

REQUIRED_ARGS = [
    "JOB_NAME",
    "REDSHIFT_CONNECTION",   # name of the Glue Data Catalog Redshift connection
    "SOURCE_DATABASE",       # Glue Data Catalog database over the raw S3/Snowflake export
    "QUALITY_RESULTS_TABLE",
    "PIPELINE_RUNS_TABLE",
]


def main() -> None:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # --- Read: Glue Data Catalog -> DynamicFrame -> Spark DataFrame ---
    # DynamicFrames handle schema drift/semi-structured data gracefully,
    # but the window-function transforms need a fixed-schema Spark
    # DataFrame, hence the .toDF() conversion at the boundary.
    quality_results_df = glue_context.create_dynamic_frame.from_catalog(
        database=args["SOURCE_DATABASE"], table_name=args["QUALITY_RESULTS_TABLE"]
    ).toDF()

    pipeline_runs_df = glue_context.create_dynamic_frame.from_catalog(
        database=args["SOURCE_DATABASE"], table_name=args["PIPELINE_RUNS_TABLE"]
    ).toDF()

    # --- Transform: pure PySpark logic, identical to local test runs ---
    anomaly_scored = compute_rolling_anomaly_scores(
        quality_results_df.filter(quality_results_df.check_name == "distribution_check"),
        value_col="score",
        partition_col="check_name",
        order_col="checked_at",
        window_size=10,
    )

    quality_summary = compute_quality_score_summary(quality_results_df)

    trended = compute_score_trend(
        quality_summary.join(pipeline_runs_df, on="run_id"),
        run_id_col="run_id",
        started_at_col="started_at",
    )

    # --- Write: Spark DataFrame -> DynamicFrame -> Redshift via Glue connection ---
    from awsglue.dynamicframe import DynamicFrame

    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=DynamicFrame.fromDF(trended, glue_context, "trended_quality"),
        catalog_connection=args["REDSHIFT_CONNECTION"],
        connection_options={
            "dbtable": "marts.mart_quality_trends",
            "database": "datapulse",
        },
    )

    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=DynamicFrame.fromDF(anomaly_scored, glue_context, "anomaly_scored"),
        catalog_connection=args["REDSHIFT_CONNECTION"],
        connection_options={
            "dbtable": "marts.mart_anomalies",
            "database": "datapulse",
        },
    )

    job.commit()  # advances the Glue job bookmark so the next run doesn't reprocess these rows


if __name__ == "__main__":
    main()
