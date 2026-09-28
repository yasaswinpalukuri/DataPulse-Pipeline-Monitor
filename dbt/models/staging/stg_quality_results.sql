select
    id           as quality_result_id,
    run_id,
    check_name,
    passed,
    score,
    rows_checked,
    rows_failed,
    message,
    checked_at
from {{ source('raw', 'quality_results') }}
