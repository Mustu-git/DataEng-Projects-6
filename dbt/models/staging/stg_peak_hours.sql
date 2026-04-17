-- stg_peak_hours.sql
-- Staging view over GOLD_PEAK_HOURS

with source as (

    select * from {{ source('gold_raw', 'GOLD_PEAK_HOURS') }}

),

renamed as (

    select
        cast(pickup_hour       as integer) as pickup_hour,
        cast(day_type          as varchar) as day_type,
        cast(total_trips       as bigint)  as total_trips,
        cast(avg_duration_mins as numeric(10, 2)) as avg_duration_mins,
        cast(avg_fare          as numeric(10, 2)) as avg_fare,
        cast(total_revenue     as numeric(18, 2)) as total_revenue

    from source

    where pickup_hour between 0 and 23
      and day_type in ('weekday', 'weekend')

)

select * from renamed
