-- One row per run with its quality outcome. Severity comes from the
-- quality_check_catalog seed, so "blocking vs row-level" lives in one place
-- in the warehouse instead of being hard-coded in every mart.
with runs as (
    select * from {{ ref('stg_pipeline_runs') }}
),

results as (
    select q.*, c.severity
    from {{ ref('stg_quality_results') }} q
    left join {{ ref('quality_check_catalog') }} c using (check_name)
),

per_run as (
    select
        run_id,
        count(*)                                                        as checks_run,
        count_if(not passed)                                            as checks_failed,
        count_if(not passed and severity = 'blocking')                  as blocking_failures,
        max(iff(severity = 'row', rows_failed / nullif(rows_checked, 0), null))
                                                                        as worst_row_check_failure_rate
    from results
    group by run_id
)

select
    r.*,
    coalesce(p.checks_run, 0)          as checks_run,
    coalesce(p.checks_failed, 0)       as checks_failed,
    coalesce(p.blocking_failures, 0)   as blocking_failures,
    p.worst_row_check_failure_rate,
    r.rows_quarantined / nullif(r.rows_loaded + r.rows_quarantined, 0) as quarantine_rate
from runs r
left join per_run p using (run_id)
