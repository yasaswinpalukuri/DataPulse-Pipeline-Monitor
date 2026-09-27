"""
One daily batch run: fetch one logical day from the NYC API and overwrite
that day's partition in Snowflake raw, then log the run.

This is what cron triggers (via `docker compose run --rm ingestion`).

    python -m ingestion.run_once                    # today's calendar day, replayed from 2023
    python -m ingestion.run_once --date 2023-01-05  # explicit backfill / rerun
"""

import argparse
import os
import time
import uuid
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from ingestion.logical_date import default_logical_date, parse_logical_date
from ingestion.nyc_api_reader import fetch_trips_for_date
from warehouse.loader import log_pipeline_run, overwrite_day_partition

LOCAL_TZ = ZoneInfo("America/Toronto")  # "today" means today where the scheduler lives


def run_ingestion_cycle(logical_date: date, max_records: int | None = None) -> dict:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    start_time = time.monotonic()

    status = "success"
    rows_loaded = 0
    rows_skipped = 0
    error_message = None

    try:
        rows, rows_skipped = fetch_trips_for_date(
            logical_date, run_id=run_id, max_records=max_records
        )
        rows_loaded = overwrite_day_partition(rows, logical_date)
        if rows_loaded == 0:
            status = "partial"  # ran fine but the source had nothing for that day
    except Exception as exc:  # noqa: BLE001 -- run boundary: any failure must
        # still produce a "failed" pipeline_runs row so monitoring sees it.
        status = "failed"
        error_message = str(exc)[:1000]
        print(f"Run {run_id} failed: {exc}")

    duration_seconds = time.monotonic() - start_time
    run_record = {
        "run_id": run_id,
        "logical_date": logical_date.isoformat(),
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "source": "nyc_open_data_api",
        "rows_ingested": rows_loaded,
        "rows_failed": rows_skipped,
        "duration_seconds": round(duration_seconds, 3),
        "error_message": error_message,
    }
    log_pipeline_run(run_record)

    print(
        f"Run {run_id} [{logical_date}]: {status} | loaded={rows_loaded} "
        f"skipped={rows_skipped} duration={duration_seconds:.2f}s"
    )
    return run_record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="DataPulse daily batch ingestion")
    parser.add_argument("--date", help="Logical date YYYY-MM-DD (must be in 2023)")
    parser.add_argument(
        "--max-records", type=int,
        default=int(os.environ["MAX_RECORDS"]) if os.environ.get("MAX_RECORDS") else None,
        help="Optional cap for quick tests; default is the full day",
    )
    args = parser.parse_args(argv)

    logical_date = (
        parse_logical_date(args.date) if args.date
        else default_logical_date(datetime.now(LOCAL_TZ).date())
    )
    record = run_ingestion_cycle(logical_date, max_records=args.max_records)
    return 1 if record["status"] == "failed" else 0  # non-zero exit so cron/logs see failure


if __name__ == "__main__":
    raise SystemExit(main())
