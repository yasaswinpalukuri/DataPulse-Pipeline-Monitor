-- Business metrics per pickup date, over rows that passed the quality gate.
select
    pickup_date,
    count(*)                                          as trips,
    round(sum(total_amount), 2)                       as revenue,
    round(avg(fare_amount), 2)                        as avg_fare,
    round(avg(trip_distance), 2)                      as avg_distance_miles,
    round(avg(trip_duration_minutes), 1)              as avg_duration_minutes,
    -- Tips are only recorded for card payments, so the tip rate is card-only.
    round(
        sum(iff(payment_type_id = 1, tip_amount, 0))
        / nullif(sum(iff(payment_type_id = 1, fare_amount, 0)), 0), 4
    )                                                 as card_tip_rate
from {{ ref('stg_taxi_trips') }}
group by pickup_date
