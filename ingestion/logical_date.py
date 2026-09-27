"""
Maps a run date to the logical date the batch should process.

Why this exists: the source is a HISTORICAL dataset (2023 only), so there is
no real "yesterday" to load. Each scheduled run replays the same calendar day
in 2023 (a run on 2026-09-28 loads 2023-09-28). The logical date is always an
explicit parameter, so reruns and backfills are one command:
    python -m ingestion.run_once --date 2023-01-05

Separating "when the job runs" from "which data it processes" is the same
idea as Airflow's logical date; it is what makes a batch job rerunnable.
"""

from datetime import date

SOURCE_YEAR = 2023


def default_logical_date(run_date: date) -> date:
    """Same month/day in SOURCE_YEAR. Feb 29 falls back to Feb 28 (2023 isn't a leap year)."""
    if run_date.month == 2 and run_date.day == 29:
        return date(SOURCE_YEAR, 2, 28)
    return run_date.replace(year=SOURCE_YEAR)


def parse_logical_date(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.year != SOURCE_YEAR:
        raise ValueError(f"Source only covers {SOURCE_YEAR}; got {value}")
    return parsed
