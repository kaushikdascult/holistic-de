with trips as (select * from {{ ref("stg_taxi_trips") }})

select *
from trips
where
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