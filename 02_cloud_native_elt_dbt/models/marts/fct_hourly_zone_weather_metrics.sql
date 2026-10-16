with
    trips as (select * from {{ ref("int_taxi_trips_cleaned") }}),

    weather as (select * from {{ ref("stg_weather") }})

select
    date_trunc('hour', t.pickup_datetime) as pickup_hour,
    t.pickup_location_id as pickup_zone,
    count(*) as trip_count,
    round(avg(t.trip_duration_minutes), 2) as avg_duration_minutes,
    round(avg(t.trip_distance), 2) as avg_distance,
    round(sum(t.fare_amount), 2) as total_fare,
    round(avg(w.temperature_2m), 1) as avg_temp_c,
    round(avg(w.precipitation), 2) as avg_precipitation_mm,
    round(avg(w.wind_speed_10m), 1) as avg_wind_speed_kmh
from trips as t
left join weather as w on date_trunc('hour', t.pickup_datetime) = w.weather_hour
group by 1, 2
order by pickup_hour asc, trip_count desc