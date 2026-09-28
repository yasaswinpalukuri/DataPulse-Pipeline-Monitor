"""Concept tests: logical dates and transactional partition overwrite."""

from datetime import date

import pytest

import warehouse.loader as loader
from ingestion.logical_date import default_logical_date, parse_logical_date


def test_logical_date_replays_same_calendar_day_in_2023():
    """I learned: run date != data date. A historical source is replayed
    calendar-aligned; the date is a parameter so any day can be rerun."""
    assert default_logical_date(date(2026, 9, 28)) == date(2023, 9, 28)


def test_leap_day_falls_back_to_feb_28():
    assert default_logical_date(date(2028, 2, 29)) == date(2023, 2, 28)


def test_dates_outside_source_year_are_rejected():
    with pytest.raises(ValueError):
        parse_logical_date("2024-01-05")


def test_nullable_int_dtype_keeps_integers_as_integers():
    """I learned: a plain pandas int column with one missing value silently
    becomes float (1 -> 1.0). Nullable 'Int64' keeps ints and uses <NA>."""
    rows = [{"passenger_count": 1}, {"passenger_count": None}]
    df = loader.rows_to_stage_frame(rows)
    assert str(df["passenger_count"].dtype) == "Int64"


class _FakeCursor:
    def __init__(self, log, fail_on=None):
        self.log, self.fail_on, self.rowcount = log, fail_on, 0

    def execute(self, sql, params=None):
        self.log.append(sql.split()[0].upper())
        if self.fail_on and sql.startswith(self.fail_on):
            raise RuntimeError("simulated failure")
        if sql.startswith("INSERT"):
            self.rowcount = 2


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _patch(monkeypatch, cursor):
    from contextlib import contextmanager

    @contextmanager
    def fake_conn():
        yield _FakeConn(cursor)

    monkeypatch.setattr(loader, "get_connection", fake_conn)
    monkeypatch.setattr(loader, "write_pandas", lambda *a, **k: cursor.log.append("WRITE_PANDAS"))


def test_overwrite_is_delete_then_insert_inside_one_transaction(monkeypatch):
    """I learned: the temp-table DDL and staging happen BEFORE BEGIN (DDL
    auto-commits in Snowflake); both tables' delete+insert swap is ONE
    transaction, so trips and quarantine for a day always match."""
    log: list[str] = []
    _patch(monkeypatch, _FakeCursor(log))
    inserted = loader.overwrite_day_partition(
        [{"trip_id": "a"}, {"trip_id": "b"}], [{"trip_id": "c", "failed_checks": "x"}],
        date(2023, 1, 1),
    )
    assert inserted == 2
    assert log == [
        "CREATE", "CREATE", "WRITE_PANDAS", "WRITE_PANDAS",
        "BEGIN", "DELETE", "DELETE", "INSERT", "INSERT", "COMMIT",
    ]


def test_failed_insert_rolls_back_so_the_day_is_not_lost(monkeypatch):
    """I learned: without the transaction, a failed insert after a successful
    delete would silently erase that day from raw."""
    log: list[str] = []
    _patch(monkeypatch, _FakeCursor(log, fail_on="INSERT"))
    with pytest.raises(RuntimeError):
        loader.overwrite_day_partition([{"trip_id": "a"}], [], date(2023, 1, 1))
    assert log[-1] == "ROLLBACK" and "COMMIT" not in log


def test_empty_batch_never_touches_the_warehouse(monkeypatch):
    """I learned: an empty API response for a valid 2023 day is more likely
    an upstream problem than 'zero trips' -- keep the last good data."""
    log: list[str] = []
    _patch(monkeypatch, _FakeCursor(log))
    assert loader.overwrite_day_partition([], [], date(2023, 1, 1)) == 0
    assert log == []
