"""
Overall pipeline status from the latest runs. Pure Python, no I/O, so it is
unit-tested directly (tests/learning/test_api_concepts.py).

    down      no successful run within STALE_AFTER_HOURS, or the latest run failed
    degraded  the latest run was blocked/partial, or it quarantined too many rows
    healthy   otherwise

Why these rules: "down" means the data consumers see is going stale or the
job is crashing -- someone must act. "degraded" means the pipeline is
running and protecting the warehouse (a block or heavy quarantine), which
needs a look but isn't an outage. STALE_AFTER_HOURS matches the dbt source
freshness warning (36h) so the API and dbt agree on what "stale" means.
"""

from datetime import datetime, timezone

STALE_AFTER_HOURS = 36
QUARANTINE_WARN_RATE = 0.05


def _as_utc(value: datetime) -> datetime:
    # Snowflake returns TIMESTAMP_NTZ as naive datetimes; ours are stored in UTC.
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def compute_status(runs: list[dict], now: datetime | None = None) -> dict:
    """runs: mart_pipeline_runs rows, newest first."""
    now = now or datetime.now(timezone.utc)
    if not runs:
        return {"status": "down", "reason": "no runs recorded", "latest_run": None,
                "hours_since_last_success": None, "success_rate_7d": None}

    latest = runs[0]
    successes = [r for r in runs if r["status"] == "success"]
    last_success_at = _as_utc(successes[0]["started_at"]) if successes else None
    hours_since = (
        round((now - last_success_at).total_seconds() / 3600, 1) if last_success_at else None
    )

    week = [r for r in runs if (now - _as_utc(r["started_at"])).days < 7]
    success_rate_7d = (
        round(sum(r["status"] == "success" for r in week) / len(week), 4) if week else None
    )

    quarantine_rate = latest.get("quarantine_rate") or 0
    if hours_since is None or hours_since > STALE_AFTER_HOURS:
        status, reason = "down", f"no successful run in over {STALE_AFTER_HOURS}h"
    elif latest["status"] == "failed":
        status, reason = "down", "latest run failed"
    elif latest["status"] in ("blocked", "partial"):
        status, reason = "degraded", f"latest run was {latest['status']}"
    elif quarantine_rate > QUARANTINE_WARN_RATE:
        status, reason = "degraded", f"latest run quarantined {quarantine_rate:.1%} of rows"
    else:
        status, reason = "healthy", "latest run succeeded within the freshness window"

    return {"status": status, "reason": reason, "latest_run": latest,
            "hours_since_last_success": hours_since, "success_rate_7d": success_rate_7d}
