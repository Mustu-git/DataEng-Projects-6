-- stg_zone_demand.sql
-- Staging view over GOLD_ZONE_DEMAND

with source as (

    select * from {{ source('gold_raw', 'GOLD_ZONE_DEMAND') }}

),

renamed as (

    select
        cast(pickup_location_id as integer) as pickup_location_id,
        cast(pickup_borough     as varchar) as pickup_borough,
        cast(pickup_zone        as varchar) as pickup_zone,
        cast(total_trips        as bigint)  as total_trips,
        cast(total_revenue      as numeric(18, 2)) as total_revenue,
        cast(avg_distance_miles as numeric(10, 3)) as avg_distance_miles,
        cast(avg_fare_per_mile  as numeric(10, 2)) as avg_fare_per_mile

    from source

    where pickup_location_id is not null

)

select * from renamed
