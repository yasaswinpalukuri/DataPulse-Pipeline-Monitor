-- One row per ingestion run: what the API's /runs endpoint and the
-- dashboard's run table read.
select
    run_id,
    logical_date,
    started_at,
    completed_at,
    status,
    rows_loaded,
    rows_quarantined,
    rows_skipped,
    round(quarantine_rate, 4)               as quarantine_rate,
    round(duration_seconds, 1)              as duration_seconds,
    checks_run,
    checks_failed,
    blocking_failures,
    round(worst_row_check_failure_rate, 4)  as worst_row_check_failure_rate,
    error_message
from {{ ref('int_run_quality') }}
