-- One row per check per run: the dashboard's "which rules fail most, and
-- is it getting worse?" chart.
select
    q.run_id,
    r.logical_date,
    r.started_at,
    q.check_name,
    c.severity,
    q.passed,
    q.rows_checked,
    q.rows_failed,
    round(q.rows_failed / nullif(q.rows_checked, 0), 4) as failure_rate
from {{ ref('stg_quality_results') }} q
join {{ ref('stg_pipeline_runs') }} r using (run_id)
left join {{ ref('quality_check_catalog') }} c using (check_name)
