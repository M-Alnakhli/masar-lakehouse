-- bronze is append-only, so a re-run of land_bronze duplicates rows.
-- keep only the latest ingested copy of each trip_id.
with source as (
    select * from (
        select *, row_number() over (
            partition by trip_id order by _ingested_at desc) as _rn
        from {{ source('bronze', 'trips') }}
        where trip_id is not null
    ) where _rn = 1
)
select
    cast(trip_id as string)           as trip_id,
    cast(driver_id as string)         as driver_id,
    cast(rider_id as string)          as rider_id,
    initcap(trim(city))               as city,
    to_timestamp(start_ts)            as started_at,
    to_timestamp(end_ts)              as ended_at,
    cast(distance_km as double)       as distance_km,
    cast(fare_sar as double)          as fare_sar,
    lower(trim(status))               as status,
    cast(surge_multiplier as double)  as surge_multiplier,
    lower(trim(payment_method))       as payment_method,
    dropoff_geohash,
    _ingested_at
from source
where trip_id is not null