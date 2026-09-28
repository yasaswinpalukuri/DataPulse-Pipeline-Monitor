"""Concept tests: the 12-check Great Expectations gate (runs real GE, not mocks)."""

import pytest

from ingestion.nyc_api_reader import SOURCE_COLUMNS, map_record
from quality.gate import (
    BLOCKING,
    ROW,
    build_checks,
    expected_row_range,
    split_rows,
    validate_batch,
)

BASE = {
    "vendorid": "2", "tpep_pickup_datetime": "2023-01-01T00:32:10.000",
    "tpep_dropoff_datetime": "2023-01-01T00:40:36.000", "passenger_count": "1.0",
    "trip_distance": "0.97", "ratecodeid": "1.0", "store_and_fwd_flag": "N",
    "pulocationid": "161", "dolocationid": "141", "payment_type": "2",
    "fare_amount": "9.3", "extra": "1.0", "mta_tax": "0.5", "tip_amount": "0.0",
    "tolls_amount": "0.0", "improvement_surcharge": "1.0", "total_amount": "14.3",
    "congestion_surcharge": "2.5", "airport_fee": "0.0",
}
ALL_COLS = set(SOURCE_COLUMNS)


def _rows(n, **overrides_for_first):
    """n distinct valid trips; optional field overrides on the first one."""
    records = [{**BASE, "total_amount": str(10 + i)} for i in range(n)]
    records[0] = {**records[0], **overrides_for_first}
    return [map_record(r, run_id="r", ingested_at="t") for r in records]


def test_there_are_exactly_12_checks_5_blocking_7_row_level():
    """I learned: be exact about what the resume claims. 11 checks run on the
    batch + 1 source-schema check = 12. Location range is ONE check over two
    columns, so it's 13 GE expectations -- say that if asked."""
    checks = build_checks((1, None))
    names = [n for n, _, _ in checks] + ["source_schema_matches"]
    assert len(names) == len(set(names)) == 12
    assert sum(1 for _, sev, _ in checks if sev == BLOCKING) + 1 == 5
    assert sum(1 for _, sev, _ in checks if sev == ROW) == 7
    assert sum(len(exps) for _, _, exps in checks) + 1 == 13


def test_clean_batch_passes_and_loads_everything():
    rows = _rows(5)
    gate = validate_batch(rows, ALL_COLS, previous_total=5)
    assert not gate.blocked
    good, bad = split_rows(rows, gate)
    assert len(good) == 5 and bad == []


def test_dirty_row_is_quarantined_but_the_batch_still_loads():
    """I learned: a refund (negative fare) is a bad ROW, not a bad BATCH.
    Quarantine it with the reason; load the rest."""
    rows = _rows(5, fare_amount="-9.3", passenger_count="0")
    gate = validate_batch(rows, ALL_COLS, previous_total=5)
    assert not gate.blocked
    good, bad = split_rows(rows, gate)
    assert len(good) == 4 and len(bad) == 1
    assert set(bad[0]["failed_checks"].split(",")) == {"fare_amount_0_500", "passenger_count_1_6"}


def test_dropoff_before_pickup_is_quarantined():
    rows = _rows(3, tpep_dropoff_datetime="2023-01-01T00:10:00.000")
    gate = validate_batch(rows, ALL_COLS, previous_total=3)
    _, bad = split_rows(rows, gate)
    assert bad[0]["failed_checks"] == "dropoff_after_pickup"


def test_null_values_pass_range_checks():
    """I learned: GE column-map expectations ignore nulls. A missing
    passenger_count is a completeness question, not a validity one."""
    rows = _rows(3, passenger_count=None)
    gate = validate_batch(rows, ALL_COLS, previous_total=3)
    _, bad = split_rows(rows, gate)
    assert bad == []


def test_duplicate_trip_id_blocks_the_whole_batch():
    """I learned: duplicate identities mean the batch can't be trusted as a
    set -- that's a blocking failure, not a per-row one."""
    rows = _rows(3)
    rows.append(dict(rows[0]))
    gate = validate_batch(rows, ALL_COLS, previous_total=4)
    assert gate.blocked
    assert any(r.check_name == "trip_id_unique" and not r.passed for r in gate.results)


def test_source_schema_drift_blocks():
    """I learned: this is the check that would have caught me reading the
    Motor Vehicle Collisions dataset by mistake -- the columns wouldn't match."""
    collisions_columns = {"crash_date", "crash_time", "borough", "zip_code"}
    gate = validate_batch(_rows(3), collisions_columns, previous_total=3)
    schema = next(r for r in gate.results if r.check_name == "source_schema_matches")
    assert gate.blocked and not schema.passed


def test_truncated_day_blocks_on_row_count():
    """I learned: an API that returns 300 rows for a normal ~77k-row day
    didn't have a quiet day -- it broke. Block instead of loading a partial day."""
    gate = validate_batch(_rows(3), ALL_COLS, previous_total=76_752)
    assert gate.blocked


@pytest.mark.parametrize("previous,expected", [(None, (1, None)), (1000, (500, 2000))])
def test_row_count_band(previous, expected):
    assert expected_row_range(previous) == expected


def test_quality_score_is_share_of_rows_passing():
    rows = _rows(4, fare_amount="999")
    gate = validate_batch(rows, ALL_COLS, previous_total=4)
    fare = next(r for r in gate.results if r.check_name == "fare_amount_0_500")
    assert fare.rows_failed == 1 and fare.failure_rate == 0.25
