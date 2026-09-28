"""Concept tests: the FastAPI pipeline-health layer (no Snowflake needed)."""

from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import api.repository as repository
from api.main import app, get_repository
from api.status import compute_status

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def _run(hours_ago: float, status: str = "success", quarantine_rate: float = 0.02) -> dict:
    started = (NOW - timedelta(hours=hours_ago)).replace(tzinfo=None)  # NTZ, like Snowflake
    return {
        "run_id": f"r{hours_ago}", "logical_date": date(2023, 9, 28), "started_at": started,
        "completed_at": started, "status": status, "rows_loaded": 100, "rows_quarantined": 2,
        "rows_skipped": 0, "quarantine_rate": quarantine_rate, "duration_seconds": 60.0,
        "checks_run": 12, "checks_failed": 0, "blocking_failures": 0,
        "worst_row_check_failure_rate": 0.01, "error_message": None,
    }


@pytest.mark.parametrize(
    "runs,expected",
    [
        ([_run(2)], "healthy"),
        ([_run(2, quarantine_rate=0.09)], "degraded"),       # ran, but quarantined a lot
        ([_run(1, "blocked"), _run(25)], "degraded"),         # gate protected the warehouse
        ([_run(1, "failed"), _run(25)], "down"),              # the job itself is broken
        ([_run(40)], "down"),                                 # stale: past the 36h window
        ([], "down"),
    ],
)
def test_status_rules(runs, expected):
    """I learned: 'degraded' (pipeline working, protecting data) is different
    from 'down' (data going stale or the job crashing). Stale = 36h, the same
    threshold as the dbt source freshness warning, so both tools agree."""
    assert compute_status(runs, now=NOW)["status"] == expected


def test_success_rate_counts_only_the_last_7_days():
    runs = [_run(1), _run(30, "blocked"), _run(24 * 10, "failed")]  # last one is 10 days old
    assert compute_status(runs, now=NOW)["success_rate_7d"] == 0.5


class _FakeRepo:
    def runs(self, limit):
        return [_run(2)][:limit]

    def daily_health(self, days):
        return []

    def quality_checks(self, runs):
        return []

    def trips_daily(self, limit):
        return [{"pickup_date": date(2023, 9, 28), "trips": 118583, "revenue": 3.1e6,
                 "avg_fare": 19.4, "avg_distance_miles": 3.4, "avg_duration_minutes": 17.2,
                 "card_tip_rate": 0.21}]


@pytest.fixture
def client():
    """I learned: FastAPI dependency overrides swap the Snowflake-backed
    repository for a fake, so the HTTP layer is tested without a warehouse."""
    app.dependency_overrides[get_repository] = _FakeRepo
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_liveness_never_touches_the_warehouse(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_status_and_runs_endpoints_serialize_mart_rows(client):
    body = client.get("/pipeline/status").json()
    assert body["latest_run"]["checks_run"] == 12
    assert client.get("/pipeline/runs?limit=1").status_code == 200
    assert client.get("/metrics/trips/daily").json()[0]["trips"] == 118583


def test_query_params_are_bounded(client):
    """I learned: validate inputs at the edge -- limit=0 or limit=10000 gets a
    422 from FastAPI before any SQL runs."""
    assert client.get("/pipeline/runs?limit=0").status_code == 422
    assert client.get("/pipeline/runs?limit=10000").status_code == 422


def test_repository_caches_so_one_refresh_is_one_warehouse_query(monkeypatch):
    """I learned: every Snowflake query can resume the warehouse (60s minimum
    bill). A 60s cache makes repeated dashboard loads free."""
    calls = []

    def fake_query(sql, params):
        calls.append(sql)
        return [{"RUN_ID": "a"}]  # Snowflake returns uppercase keys

    repo = repository.MartRepository(query=fake_query)
    assert repo.runs(5) == [{"run_id": "a"}]
    repo.runs(5)
    assert len(calls) == 1

    monkeypatch.setattr(repository, "CACHE_TTL_SECONDS", 0)
    repo.runs(5)
    assert len(calls) == 2
