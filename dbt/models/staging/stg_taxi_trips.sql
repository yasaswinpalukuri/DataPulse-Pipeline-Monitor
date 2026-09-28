-- 1:1 with raw.taxi_trips: consistent names, derived duration. No filtering.
select
    trip_id,
    vendor_id,
    pickup_datetime                                              as pickup_at,
    dropoff_datetime                                             as dropoff_at,
    pickup_date,
    passenger_count,
    trip_distance,
    ratecode_id                                                  as rate_code_id,
    pu_location_id,
    do_location_id,
    payment_type                                                 as payment_type_id,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    datediff('second', pickup_datetime, dropoff_datetime) / 60.0 as trip_duration_minutes,
    run_id,
    ingested_at
from {{ source('raw', 'taxi_trips') }}
