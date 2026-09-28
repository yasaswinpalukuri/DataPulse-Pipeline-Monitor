-- rows_quarantined was added after the first runs (migration 002), so older
-- runs have NULL there; they quarantined nothing, so 0 is the true value.
select
    run_id,
    logical_date,
    started_at,
    completed_at,
    lower(status)                  as status,
    source,
    rows_ingested                  as rows_loaded,
    coalesce(rows_quarantined, 0)  as rows_quarantined,
    rows_failed                    as rows_skipped,
    duration_seconds,
    error_message
from {{ source('raw', 'pipeline_runs') }}
