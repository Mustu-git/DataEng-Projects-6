-- stg_daily_trips.sql
-- Staging view over the raw GOLD_DAILY_TRIPS table written by Spark.
-- Casts, renames, and light validation only — no business logic here.

with source as (

    select * from {{ source('gold_raw', 'GOLD_DAILY_TRIPS') }}

),

renamed as (

    select
        cast(trip_date        as date)    as trip_date,
        cast(total_trips      as bigint)  as total_trips,
        cast(total_revenue    as numeric(18, 2)) as total_revenue,
        cast(avg_fare         as numeric(10, 2)) as avg_fare,
        cast(avg_distance_miles as numeric(10, 3)) as avg_distance_miles,
        cast(avg_duration_mins  as numeric(10, 2)) as avg_duration_mins

    from source

    -- Guard against Spark writing rows with null dates
    where trip_date is not null
      and trip_date between '2019-01-01' and '2024-12-31'

)

select * from renamed
