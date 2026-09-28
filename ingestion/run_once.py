"""
One daily batch run:
    fetch one logical day -> quality gate (12 GE checks) ->
        blocked?  alert, load nothing
        else:     load good rows + quarantine bad rows (one transaction),
                  alert if any row-level check fails on too many rows
    -> log per-check results and the run itself.

Statuses in raw.pipeline_runs: success | blocked | partial | failed.

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

from dotenv import load_dotenv

from alerts.slack import send_alert
from ingestion.logical_date import default_logical_date, parse_logical_date
from ingestion.nyc_api_reader import fetch_trips_for_date
from quality.gate import GateResult, split_rows, validate_batch
from warehouse.loader import (
    log_pipeline_run,
    log_quality_results,
    overwrite_day_partition,
    previous_run_total,
)

LOCAL_TZ = ZoneInfo("America/Toronto")  # "today" means today where the scheduler lives


def _quality_records(gate: GateResult, run_id: str) -> list[dict]:
    checked_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            "id": str(uuid.uuid4()),
            "run_id": run_id,
            "check_name": r.check_name,
            "passed": r.passed,
            "score": round(1 - r.failure_rate, 6),
            "rows_checked": r.rows_checked,
            "rows_failed": r.rows_failed,
            "message": r.message,
            "checked_at": checked_at,
        }
        for r in gate.results
    ]


def _format_failures(results) -> str:
    return "\n".join(
        f"• `{r.check_name}`: {r.rows_failed}/{r.rows_checked} rows ({r.failure_rate:.1%}) {r.message}"
        if r.rows_checked else f"• `{r.check_name}`: {r.message}"
        for r in results
    )


def run_ingestion_cycle(logical_date: date, max_records: int | None = None) -> dict:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    start_time = time.monotonic()

    status = "success"
    rows_loaded = rows_quarantined = rows_skipped = 0
    error_message = None

    try:
        rows, rows_skipped, observed_columns = fetch_trips_for_date(
            logical_date, run_id=run_id, max_records=max_records
        )
        gate = validate_batch(rows, observed_columns, previous_run_total())
        log_quality_results(_quality_records(gate, run_id))

        if gate.blocked:
            status = "blocked"
            failed = [r for r in gate.results if r.severity == "blocking" and not r.passed]
            error_message = "; ".join(f"{r.check_name}: {r.message}" for r in failed)[:1000]
            send_alert(
                f":rotating_light: *DataPulse load BLOCKED* for {logical_date} "
                f"(run `{run_id[:8]}`). Nothing was loaded.\n{_format_failures(failed)}"
            )
        else:
            good, quarantined = split_rows(rows, gate)
            rows_quarantined = len(quarantined)
            rows_loaded = overwrite_day_partition(good, quarantined, logical_date)
            if gate.row_alerts:
                send_alert(
                    f":warning: *DataPulse data-quality warning* for {logical_date}: "
                    f"loaded {rows_loaded}, quarantined {rows_quarantined}.\n"
                    f"{_format_failures(gate.row_alerts)}"
                )
    except Exception as exc:  # noqa: BLE001 -- run boundary: any failure must
        # still produce a "failed" pipeline_runs row so monitoring sees it.
        status = "failed"
        error_message = str(exc)[:1000]
        print(f"Run {run_id} failed: {exc}")
        send_alert(f":x: *DataPulse run FAILED* for {logical_date}: {error_message[:300]}")

    duration_seconds = time.monotonic() - start_time
    run_record = {
        "run_id": run_id,
        "logical_date": logical_date.isoformat(),
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "source": "nyc_open_data_api",
        "rows_ingested": rows_loaded,
        "rows_quarantined": rows_quarantined,
        "rows_failed": rows_skipped,
        "duration_seconds": round(duration_seconds, 3),
        "error_message": error_message,
    }
    log_pipeline_run(run_record)

    print(
        f"Run {run_id} [{logical_date}]: {status} | loaded={rows_loaded} "
        f"quarantined={rows_quarantined} skipped={rows_skipped} "
        f"duration={duration_seconds:.2f}s"
    )
    return run_record


def main(argv: list[str] | None = None) -> int:
    # Config is loaded at the process boundary, not on import. Inside Docker
    # the variables already exist (env_file) and load_dotenv() doesn't
    # override them; on a laptop it fills them in from .env.
    load_dotenv()
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
    return 1 if record["status"] in ("failed", "blocked") else 0  # non-zero exit for cron/logs


if __name__ == "__main__":
    raise SystemExit(main())
