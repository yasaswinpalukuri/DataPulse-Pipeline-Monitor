select
    trip_id,
    pickup_date,
    failed_checks,
    run_id
from {{ source('raw', 'taxi_trips_quarantine') }}
