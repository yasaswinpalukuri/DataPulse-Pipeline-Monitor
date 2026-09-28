-- Health per calendar day the pipeline RAN (started_at), not per logical
-- date: this answers "how did the pipeline behave on Monday?", including
-- manual backfills of older dates.
select
    to_date(started_at)                                        as run_date,
    count(*)                                                   as runs,
    count_if(status = 'success')                               as successful_runs,
    count_if(status = 'blocked')                               as blocked_runs,
    count_if(status = 'failed')                                as failed_runs,
    round(count_if(status = 'success') / count(*), 4)          as success_rate,
    sum(rows_loaded)                                           as rows_loaded,
    sum(rows_quarantined)                                      as rows_quarantined,
    round(avg(duration_seconds), 1)                            as avg_duration_seconds,
    round(avg(quarantine_rate), 4)                             as avg_quarantine_rate
from {{ ref('int_run_quality') }}
group by 1
