"""
Read-only access to the dbt marts, with a small TTL cache.

Why the API reads ONLY marts: they are dbt's tested serving contract. Reading
raw tables here would bypass those tests and duplicate modelling logic.

Why a cache: every Snowflake query can resume the warehouse, and a resume
bills at least 60 seconds. One dashboard refresh fires several requests;
without the cache that's several billing windows. The data only changes once
a day, so 60 seconds of staleness costs nothing.

Why a class with injectable query function: the API depends on
`MartRepository`, and tests swap in a fake via FastAPI's dependency
overrides -- no Snowflake needed to test the HTTP layer.
"""

import threading
import time
from collections.abc import Callable

from warehouse.snowflake_client import run_query

CACHE_TTL_SECONDS = 60

RUNS_SQL = """
    SELECT * FROM datapulse.marts.mart_pipeline_runs
    ORDER BY started_at DESC
    LIMIT %s
"""
DAILY_HEALTH_SQL = """
    SELECT * FROM datapulse.marts.mart_pipeline_health_daily
    ORDER BY run_date DESC
    LIMIT %s
"""
CHECKS_FOR_RECENT_RUNS_SQL = """
    SELECT * FROM datapulse.marts.mart_quality_check_trends
    WHERE run_id IN (
        SELECT DISTINCT run_id FROM datapulse.marts.mart_quality_check_trends
        ORDER BY started_at DESC LIMIT %s
    )
    ORDER BY started_at DESC, check_name
"""
TRIPS_DAILY_SQL = """
    SELECT * FROM datapulse.marts.mart_daily_trip_metrics
    ORDER BY pickup_date DESC
    LIMIT %s
"""


class MartRepository:
    def __init__(self, query: Callable[[str, tuple], list[dict]] = run_query) -> None:
        self._query = query
        self._cache: dict[tuple, tuple[float, list[dict]]] = {}
        self._lock = threading.Lock()

    def _fetch(self, sql: str, params: tuple) -> list[dict]:
        key = (sql, params)
        with self._lock:
            hit = self._cache.get(key)
            if hit and time.monotonic() - hit[0] < CACHE_TTL_SECONDS:
                return hit[1]
        # Snowflake returns UPPERCASE column names; the API speaks lowercase.
        rows = [{k.lower(): v for k, v in row.items()} for row in self._query(sql, params)]
        with self._lock:
            self._cache[key] = (time.monotonic(), rows)
        return rows

    def runs(self, limit: int) -> list[dict]:
        return self._fetch(RUNS_SQL, (limit,))

    def daily_health(self, days: int) -> list[dict]:
        return self._fetch(DAILY_HEALTH_SQL, (days,))

    def quality_checks(self, runs: int) -> list[dict]:
        return self._fetch(CHECKS_FOR_RECENT_RUNS_SQL, (runs,))

    def trips_daily(self, limit: int) -> list[dict]:
        return self._fetch(TRIPS_DAILY_SQL, (limit,))
