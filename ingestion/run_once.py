"""
Runs a single ingestion cycle: fetch from NYC API, load to Snowflake,
log the run outcome. This is the synchronous building block that
Day 3's asyncio scheduler will call on a loop -- written standalone
first so it's testable and runnable without the scheduler existing yet.

Run manually: python -m ingestion.run_once
"""

import time
import uuid
from datetime import datetime, timezone

from ingestion.nyc_api_reader import fetch_trips
from warehouse.loader import load_taxi_trips, log_pipeline_run


def run_ingestion_cycle(max_records: int = 1000) -> dict:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    start_time = time.monotonic()

    status = "success"
    rows_inserted = 0
    rows_skipped = 0

    try:
        mapped_rows, skipped = fetch_trips(run_id=run_id, max_records=max_records)
        rows_skipped = skipped
        rows_inserted = load_taxi_trips(mapped_rows)
        if rows_inserted == 0 and max_records > 0:
            status = "partial"
    except Exception as exc:  # noqa: BLE001 -- top-level cycle boundary:
        # any failure (network, mapping, Snowflake) must still result in
        # a logged "failed" pipeline_runs row rather than crashing the
        # scheduler that calls this on a loop.
        status = "failed"
        print(f"Ingestion run {run_id} failed: {exc}")

    duration_seconds = time.monotonic() - start_time
    completed_at = datetime.now(timezone.utc)

    run_record = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "status": status,
        "source": "nyc_open_data_api",
        "rows_ingested": rows_inserted,
        "rows_failed": rows_skipped,
        "duration_seconds": round(duration_seconds, 3),
    }
    log_pipeline_run(run_record)

    print(
        f"Run {run_id}: {status} | inserted={rows_inserted} "
        f"skipped={rows_skipped} duration={duration_seconds:.2f}s"
    )
    return run_record


if __name__ == "__main__":
    run_ingestion_cycle()
