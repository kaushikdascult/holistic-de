with
    source_data as (select * from {{ source("raw_lakehouse", "raw_taxi") }})

select
    vendorid as vendor_id,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    round(
        date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0,
        2
    ) as trip_duration_minutes,
    trip_distance,
    pulocationid as pickup_location_id,
    dolocationid as dropoff_location_id,
    coalesce(passenger_count, 1) as passenger_count,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee as airport_fee,
    cbd_congestion_fee,
    total_amount,
    payment_type
from source_data