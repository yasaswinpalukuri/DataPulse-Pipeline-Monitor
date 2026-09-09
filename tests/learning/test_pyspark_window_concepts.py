"""
Runs a REAL local Spark session (not mocked) against
transforms/quality_transforms_core.py, to prove the window function
logic behaves as documented -- specifically that rowsBetween(-N, -1)
excludes the current row from its own baseline, which is the crux of
why this isn't a look-ahead-biased anomaly detector.
"""

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from transforms.quality_transforms_core import (
    compute_quality_score_summary,
    compute_rolling_anomaly_scores,
    compute_score_trend,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("datapulse-concept-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_rolling_window_excludes_current_row_from_its_own_baseline(spark):
    """
    I learned: rowsBetween(-window_size, -1) -- ending at -1, not 0 --
    means the row being scored is NOT included in the mean/stddev it's
    compared against. This test proves it: a huge spike's own value
    must NOT appear in its rolling_mean.
    """
    data = [
        ("fare_amount", 1, 12.0),
        ("fare_amount", 2, 13.0),
        ("fare_amount", 3, 11.0),
        ("fare_amount", 4, 12.5),
        ("fare_amount", 5, 9999.0),  # spike
    ]
    df = spark.createDataFrame(data, ["column_name", "run_seq", "value"])

    result = compute_rolling_anomaly_scores(
        df,
        value_col="value",
        partition_col="column_name",
        order_col="run_seq",
        window_size=10,
    )
    spike_row = result.filter(result.run_seq == 5).collect()[0]

    # If the spike's own 9999.0 were included in its baseline, the
    # rolling_mean would be pulled way up. It must instead reflect only
    # the 4 prior normal values (~12.125).
    assert spike_row["rolling_mean"] < 20
    assert spike_row["severity"] == "high"
    assert spike_row["is_anomaly"] is True


def test_first_row_in_partition_has_no_baseline_yet(spark):
    """
    I learned: a row with fewer than 2 prior observations gets a NULL
    z_score/severity rather than a fabricated one -- Spark's stddev
    over a single point (or zero points) is NULL, and the transform
    passes that NULL through rather than treating it as "0 = not
    anomalous," which would be a false signal.

    Reuses the shared module-scoped `spark` fixture rather than
    creating and stopping its own session -- an earlier version of
    this test did that, which stopped the single underlying JVM
    SparkContext out from under the other tests in this file that
    still needed it. There's one JVM context per process; only the
    fixture that created it should ever stop it.
    """
    df = spark.createDataFrame(
        [("fare_amount", 1, 12.0)], ["column_name", "run_seq", "value"]
    )
    result = compute_rolling_anomaly_scores(
        df, value_col="value", partition_col="column_name",
        order_col="run_seq", window_size=10,
    )
    first_row = result.collect()[0]
    assert first_row["z_score"] is None
    assert first_row["severity"] is None


def test_quality_score_summary_matches_manual_average(spark):
    """
    I learned: groupBy().agg(F.avg(...)) is the Spark-native way to do
    what dbt's int_quality_scores model does in SQL -- same aggregation
    semantics, different execution engine.
    """
    data = [
        ("run_1", "null_check", True, 1.0),
        ("run_1", "range_check", True, 0.9),
        ("run_1", "freshness_check", False, 0.4),
    ]
    df = spark.createDataFrame(data, ["run_id", "check_name", "passed", "score"])

    result = compute_quality_score_summary(df).collect()[0]
    manual_avg = (1.0 + 0.9 + 0.4) / 3

    assert abs(result["quality_score"] - manual_avg) < 0.0001
    assert result["checks_failed"] == 1
    assert abs(result["pass_rate"] - (2 / 3)) < 0.0001


def test_trend_uses_single_row_lag_not_rolling_window(spark):
    """
    I learned: F.lag() over Window.orderBy() looks back exactly ONE
    row -- deliberately different machinery from the rolling z-score
    window above, because "trend vs. last run" and "anomaly vs. last
    10 runs" are different questions with different correct window
    shapes. Using the same rolling window for both would answer the
    wrong question for at least one of them.
    """
    data = [
        ("run_1", "2024-01-01", 0.95),
        ("run_2", "2024-01-02", 0.80),
        ("run_3", "2024-01-03", 0.85),
    ]
    df = spark.createDataFrame(data, ["run_id", "started_at", "quality_score"])

    result = compute_score_trend(df, "run_id", "started_at").orderBy("started_at").collect()

    assert result[0]["trend"] == "baseline"  # no previous run
    assert result[1]["trend"] == "declining"  # 0.80 < 0.95
    assert result[2]["trend"] == "improving"  # 0.85 > 0.80
