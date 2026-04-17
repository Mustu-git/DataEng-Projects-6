-- Custom singular test: every day should have at least 1 trip.
-- Any row with total_trips = 0 would indicate a bad aggregation.

select *
from {{ ref('stg_daily_trips') }}
where total_trips <= 0
