with trips as (select * from {{ ref("stg_taxi_trips") }})

select
    *,
    case
        when
            pickup_datetime < '2026-01-01 00:00:00'
            or pickup_datetime >= '2026-02-01 00:00:00'
        then 'OUT_OF_BOUNDS_PICKUP_TIMESTAMP'
        when dropoff_datetime <= pickup_datetime
        then 'INVALID_DROPOFF_BEFORE_PICKUP'
        when trip_duration_minutes < 1.0
        then 'TRIP_TOO_SHORT_UNDER_1_MIN'
        when trip_duration_minutes > 1440.0
        then 'TRIP_TOO_LONG_OVER_24_HOURS'
        when trip_distance <= 0.0 or trip_distance >= 100.0
        then 'INVALID_TRIP_DISTANCE'
        when fare_amount < 2.50 or fare_amount >= 500.0
        then 'INVALID_FARE_AMOUNT'
        when total_amount <= 0.0
        then 'INVALID_TOTAL_AMOUNT'
        when
            pickup_location_id not between 1 and 263
            or dropoff_location_id not between 1 and 263
        then 'UNKNOWN_OR_OUT_OF_BOUNDS_ZONE'
        else 'UNKNOWN_DEFECT'
    end as dq_status
from trips
where
    not (
        pickup_datetime >= '2026-01-01 00:00:00'
        and pickup_datetime < '2026-02-01 00:00:00'
        and dropoff_datetime > pickup_datetime
        and trip_duration_minutes >= 1.0
        and trip_duration_minutes <= 1440.0
        and trip_distance > 0.0
        and trip_distance < 100.0
        and fare_amount >= 2.50
        and fare_amount < 500.0
        and total_amount > 0.0
        and pickup_location_id between 1 and 263
        and dropoff_location_id between 1 and 263
    )