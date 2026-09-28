"""
Great Expectations quality gate for one daily batch: 12 checks in two tiers.

Why two tiers instead of "any failure blocks the load":
real TLC data is dirty by design -- refunds show up as negative fares, some
trips report 0 passengers, meters glitch. If every check were blocking, the
pipeline would essentially never load. So checks are split by WHAT a failure
means:

- BLOCKING (batch-level): the batch itself is untrustworthy -- the source
  schema changed, identity is broken (null/duplicate trip_id), or the row
  count is wildly off vs the previous run (e.g. an API returning an empty or
  truncated day). Nothing is loaded, the run is marked 'blocked', Slack is
  alerted. These should almost never fire; when they do, loading would be wrong.

- ROW-LEVEL (quarantine): individual rows break a business rule. Those rows
  go to raw.taxi_trips_quarantine with the names of the checks they failed;
  every other row loads. Slack is alerted when any check's failure rate
  crosses ROW_ALERT_THRESHOLD. Nothing is silently dropped -- quarantined
  rows are queryable and countable.

Why GE instead of hand-written pandas filters: expectations are declarative
and self-describing (the result says which rule, how many rows, which
indices), and the same suite definitions produce the per-check metrics we
persist to raw.quality_results for the dashboard.

Counting note (be precise if asked): 12 checks = 13 GE expectations, because
"pickup/dropoff LocationID in 1-265" is one check applied to two columns.
"""

import os
import warnings
from dataclasses import dataclass, field

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

from ingestion.nyc_api_reader import SOURCE_COLUMNS

BLOCKING = "blocking"
ROW = "row"

ROW_ALERT_THRESHOLD = float(os.environ.get("ROW_ALERT_THRESHOLD", "0.05"))

# Row-count tolerance vs the previous run. Daily taxi volume swings a lot
# (holidays, weekends), so the band is wide: it catches "API returned 300 rows"
# or "doubled because of a bug", not normal day-to-day variation.
ROW_COUNT_LOWER_RATIO = 0.5
ROW_COUNT_UPPER_RATIO = 2.0

VALID_PAYMENT_TYPES = [1, 2, 3, 4, 5, 6]          # TLC data dictionary
VALID_RATE_CODES = [1, 2, 3, 4, 5, 6, 99]          # 99 = null/unknown in 2023 dictionary


@dataclass
class CheckResult:
    check_name: str
    severity: str
    passed: bool
    rows_checked: int
    rows_failed: int
    message: str

    @property
    def failure_rate(self) -> float:
        return self.rows_failed / self.rows_checked if self.rows_checked else 0.0


@dataclass
class GateResult:
    results: list[CheckResult]
    failed_checks_by_row: dict[int, list[str]] = field(default_factory=dict)

    @property
    def blocked(self) -> bool:
        return any(r.severity == BLOCKING and not r.passed for r in self.results)

    @property
    def row_alerts(self) -> list[CheckResult]:
        return [
            r for r in self.results
            if r.severity == ROW and r.failure_rate > ROW_ALERT_THRESHOLD
        ]


def expected_row_range(previous_total: int | None) -> tuple[int, int | None]:
    """Bounds for today's row count. No history yet -> just require at least one row."""
    if not previous_total:
        return 1, None
    return int(previous_total * ROW_COUNT_LOWER_RATIO), int(previous_total * ROW_COUNT_UPPER_RATIO)


def build_checks(row_range: tuple[int, int | None]) -> list[tuple[str, str, list]]:
    """The 12 checks as (name, severity, [expectations]). Order = report order."""
    lo, hi = row_range
    between = gxe.ExpectColumnValuesToBeBetween
    return [
        # --- blocking: is this batch trustworthy at all? ---
        ("trip_id_not_null", BLOCKING, [gxe.ExpectColumnValuesToNotBeNull(column="trip_id")]),
        ("trip_id_unique", BLOCKING, [gxe.ExpectColumnValuesToBeUnique(column="trip_id")]),
        ("pickup_datetime_not_null", BLOCKING,
         [gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime")]),
        ("row_count_vs_previous_run", BLOCKING,
         [gxe.ExpectTableRowCountToBeBetween(min_value=lo, max_value=hi)]),
        # (the 5th blocking check, source_schema_matches, runs on the source
        #  column set -- see _source_schema_check)
        # --- row-level: quarantine the row, keep the batch ---
        ("dropoff_after_pickup", ROW, [gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="dropoff_datetime", column_B="pickup_datetime",
            or_equal=False, ignore_row_if="either_value_is_missing")]),
        ("fare_amount_0_500", ROW, [between(column="fare_amount", min_value=0, max_value=500)]),
        ("trip_distance_0_100", ROW,
         [between(column="trip_distance", min_value=0, max_value=100)]),
        ("passenger_count_1_6", ROW,
         [between(column="passenger_count", min_value=1, max_value=6)]),
        ("payment_type_valid", ROW, [gxe.ExpectColumnValuesToBeInSet(
            column="payment_type", value_set=VALID_PAYMENT_TYPES)]),
        ("ratecode_id_valid", ROW, [gxe.ExpectColumnValuesToBeInSet(
            column="ratecode_id", value_set=VALID_RATE_CODES)]),
        ("location_ids_1_265", ROW, [
            between(column="pu_location_id", min_value=1, max_value=265),
            between(column="do_location_id", min_value=1, max_value=265),
        ]),
    ]


def _validation_frame(rows: list[dict]) -> pd.DataFrame:
    """Typed copy of the batch for validation only (the load path is untouched).

    Datetimes are parsed so 'dropoff > pickup' compares instants, not strings;
    unparseable timestamps become NaT and fail the not-null check. Integer codes
    become float so GE sees plain numbers with NaN for missing.
    Note: GE column-map expectations IGNORE nulls -- a null passenger_count
    passes the 1-6 range check. Missingness is a separate question from
    validity, and we only gate on nulls where they break identity.
    """
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["trip_id", "pickup_datetime", "dropoff_datetime"])
    for col in ("pickup_datetime", "dropoff_datetime"):
        if col in df:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _source_schema_check(observed_columns: set[str]) -> CheckResult:
    """Blocking check 5: the API still returns the columns our contract expects.

    Runs on the set of keys seen across the source records, not on our mapped
    frame (which we build ourselves, so checking it would be circular). This
    is the check that would have caught the early bug where the pipeline read
    the Motor Vehicle Collisions dataset by mistake.
    """
    ctx = gx.get_context(mode="ephemeral")
    batch_def = (
        ctx.data_sources.add_pandas("source")
        .add_dataframe_asset("source_columns")
        .add_batch_definition_whole_dataframe("columns")
    )
    batch = batch_def.get_batch(
        batch_parameters={"dataframe": pd.DataFrame(columns=sorted(observed_columns))}
    )
    suite = gx.ExpectationSuite(name="source_contract")
    suite.add_expectation(
        gxe.ExpectTableColumnsToMatchSet(column_set=SOURCE_COLUMNS, exact_match=True)
    )
    result = batch.validate(suite).results[0]
    missing = sorted(set(SOURCE_COLUMNS) - observed_columns)
    extra = sorted(observed_columns - set(SOURCE_COLUMNS))
    return CheckResult(
        "source_schema_matches", BLOCKING, bool(result.success),
        rows_checked=0, rows_failed=0,
        message="ok" if result.success else f"missing={missing} extra={extra}",
    )


def validate_batch(
    rows: list[dict], observed_columns: set[str], previous_total: int | None
) -> GateResult:
    """Run all 12 checks. Returns per-check results plus which rows failed which row checks."""
    warnings.filterwarnings("ignore", module="great_expectations")
    df = _validation_frame(rows)

    ctx = gx.get_context(mode="ephemeral")
    batch = (
        ctx.data_sources.add_pandas("batch")
        .add_dataframe_asset("taxi_trips")
        .add_batch_definition_whole_dataframe("whole")
        .get_batch(batch_parameters={"dataframe": df})
    )

    checks = build_checks(expected_row_range(previous_total))
    suite = gx.ExpectationSuite(name="raw_taxi_trips")
    for name, _, expectations in checks:
        for exp in expectations:
            exp.meta = {"check": name}
            suite.add_expectation(exp)
    validation = batch.validate(suite, result_format="COMPLETE")

    by_check: dict[str, list] = {}
    for res in validation.results:
        by_check.setdefault(res.expectation_config.meta["check"], []).append(res)

    results = [_source_schema_check(observed_columns)]
    failed_by_row: dict[int, list[str]] = {}
    for name, severity, _ in checks:
        exp_results = by_check[name]
        passed = all(r.success for r in exp_results)
        bad_rows: set[int] = set()
        for r in exp_results:
            bad_rows.update(r.result.get("unexpected_index_list") or [])
        if severity == ROW:
            for idx in bad_rows:
                failed_by_row.setdefault(idx, []).append(name)
        observed = exp_results[0].result.get("observed_value")
        results.append(CheckResult(
            name, severity, passed,
            rows_checked=len(df), rows_failed=len(bad_rows),
            message=f"observed={observed}" if observed is not None
            else f"{len(bad_rows)} of {len(df)} rows failed",
        ))
    return GateResult(results=results, failed_checks_by_row=failed_by_row)


def split_rows(rows: list[dict], gate: GateResult) -> tuple[list[dict], list[dict]]:
    """(rows to load, rows to quarantine with a failed_checks column)."""
    good, quarantined = [], []
    for idx, row in enumerate(rows):
        failed = gate.failed_checks_by_row.get(idx)
        if failed:
            quarantined.append({**row, "failed_checks": ",".join(failed)})
        else:
            good.append(row)
    return good, quarantined
