-- mart_monthly_revenue.sql  (incremental)
--
-- Rolls up daily trip data to a monthly grain.
-- Incremental strategy: on each run, only process months that are new or
-- have changed — Snowflake only scans/inserts new months, not the whole table.
-- This is the dbt pattern that "reduces warehouse compute on reruns."

{{
    config(
        materialized  = 'incremental',
        unique_key    = 'month_start_date',
        incremental_strategy = 'merge',
        on_schema_change = 'sync_all_columns'
    )
}}

with daily as (

    select * from {{ ref('stg_daily_trips') }}

    -- Incremental filter: only process rows newer than the latest month already loaded
    {% if is_incremental() %}
        where date_trunc('month', trip_date) > (
            select coalesce(max(month_start_date), '2018-12-01')
            from {{ this }}
        )
    {% endif %}

),

monthly as (

    select
        date_trunc('month', trip_date)::date            as month_start_date,
        year(trip_date)                                  as trip_year,
        month(trip_date)                                 as trip_month,
        sum(total_trips)                                 as total_trips,
        round(sum(total_revenue), 2)                     as total_revenue,
        round(avg(avg_fare), 2)                          as avg_fare,
        round(avg(avg_distance_miles), 3)                as avg_distance_miles,
        round(avg(avg_duration_mins), 2)                 as avg_duration_mins,
        count(distinct trip_date)                        as days_with_data

    from daily
    group by 1, 2, 3

)

select * from monthly
