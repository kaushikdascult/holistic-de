with
    raw_weather as (select * from {{ source("raw_lakehouse", "raw_weather") }})

select
    cast(unnest(hourly.time) as timestamp) as weather_hour,
    unnest(hourly.temperature_2m) as temperature_2m,
    unnest(hourly.precipitation) as precipitation,
    unnest(hourly.snowfall) as snowfall,
    unnest(hourly.wind_speed_10m) as wind_speed_10m
from raw_weather