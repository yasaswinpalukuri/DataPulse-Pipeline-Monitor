"""
DataPulse pipeline-health API (FastAPI). Serves the dbt marts; never raw tables.

    GET /health                liveness: the process is up (no Snowflake call)
    GET /health/ready          readiness: Snowflake is reachable
    GET /pipeline/status       healthy | degraded | down, with the reason
    GET /pipeline/runs         recent runs with quality outcome
    GET /pipeline/health/daily per-day success rate, rows, quarantine rate
    GET /quality/checks        per-check results for the last N gated runs
    GET /metrics/trips/daily   trips, revenue and averages per pickup date

Interactive docs at /docs (generated from the response models below).

Why liveness and readiness are separate: if /health called Snowflake, a
warehouse hiccup would make an orchestrator restart a perfectly healthy API
container. Liveness answers "is this process alive?", readiness answers
"can it serve data right now?".
"""

from datetime import date, datetime
from functools import lru_cache

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel

from api.repository import MartRepository
from api.status import compute_status
from warehouse.snowflake_client import test_connection

load_dotenv()  # entry point: fills env vars when run outside Docker

app = FastAPI(
    title="DataPulse Pipeline Health API",
    version="1.0.0",
    description="Pipeline-health and trip metrics served from the dbt marts in Snowflake.",
)


class Run(BaseModel):
    run_id: str
    logical_date: date | None
    started_at: datetime
    completed_at: datetime | None
    status: str
    rows_loaded: int | None
    rows_quarantined: int | None
    rows_skipped: int | None
    quarantine_rate: float | None
    duration_seconds: float | None
    checks_run: int
    checks_failed: int
    blocking_failures: int
    worst_row_check_failure_rate: float | None
    error_message: str | None


class PipelineStatus(BaseModel):
    status: str
    reason: str
    hours_since_last_success: float | None
    success_rate_7d: float | None
    latest_run: Run | None


class DailyHealth(BaseModel):
    run_date: date
    runs: int
    successful_runs: int
    blocked_runs: int
    failed_runs: int
    success_rate: float
    rows_loaded: int | None
    rows_quarantined: int | None
    avg_duration_seconds: float | None
    avg_quarantine_rate: float | None


class CheckResult(BaseModel):
    run_id: str
    logical_date: date | None
    started_at: datetime
    check_name: str
    severity: str | None
    passed: bool
    rows_checked: int | None
    rows_failed: int | None
    failure_rate: float | None


class TripDay(BaseModel):
    pickup_date: date
    trips: int
    revenue: float | None
    avg_fare: float | None
    avg_distance_miles: float | None
    avg_duration_minutes: float | None
    card_tip_rate: float | None


@lru_cache
def get_repository() -> MartRepository:
    """One repository (and one cache) per process. Tests override this."""
    return MartRepository()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/health/ready")
def ready() -> dict:
    if not test_connection():
        raise HTTPException(status_code=503, detail="Snowflake unreachable")
    return {"status": "ready"}


@app.get("/pipeline/status", response_model=PipelineStatus)
def pipeline_status(repo: MartRepository = Depends(get_repository)) -> dict:
    return compute_status(repo.runs(limit=200))


@app.get("/pipeline/runs", response_model=list[Run])
def pipeline_runs(
    limit: int = Query(20, ge=1, le=200), repo: MartRepository = Depends(get_repository)
) -> list[dict]:
    return repo.runs(limit)


@app.get("/pipeline/health/daily", response_model=list[DailyHealth])
def daily_health(
    days: int = Query(14, ge=1, le=90), repo: MartRepository = Depends(get_repository)
) -> list[dict]:
    return repo.daily_health(days)


@app.get("/quality/checks", response_model=list[CheckResult])
def quality_checks(
    runs: int = Query(5, ge=1, le=50), repo: MartRepository = Depends(get_repository)
) -> list[dict]:
    return repo.quality_checks(runs)


@app.get("/metrics/trips/daily", response_model=list[TripDay])
def trips_daily(
    limit: int = Query(30, ge=1, le=366), repo: MartRepository = Depends(get_repository)
) -> list[dict]:
    return repo.trips_daily(limit)
