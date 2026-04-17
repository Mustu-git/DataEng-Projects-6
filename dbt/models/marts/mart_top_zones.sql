-- mart_top_zones.sql  (incremental, zone-level summary)
--
-- Enriches zone demand with revenue-per-trip and ranks zones within each borough.
-- Incremental key: pickup_location_id (upsert on each run handles Spark reruns).

{{
    config(
        materialized  = 'incremental',
        unique_key    = 'pickup_location_id',
        incremental_strategy = 'merge',
        on_schema_change = 'sync_all_columns'
    )
}}

with zones as (

    select * from {{ ref('stg_zone_demand') }}

),

enriched as (

    select
        pickup_location_id,
        pickup_borough,
        pickup_zone,
        total_trips,
        total_revenue,
        avg_distance_miles,
        avg_fare_per_mile,
        round(total_revenue / nullif(total_trips, 0), 2)    as revenue_per_trip,
        rank() over (
            partition by pickup_borough
            order by total_trips desc
        )                                                     as rank_in_borough,
        rank() over (
            order by total_trips desc
        )                                                     as overall_rank

    from zones

)

select * from enriched
