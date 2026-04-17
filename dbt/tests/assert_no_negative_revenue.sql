-- Custom singular test: no row in mart_monthly_revenue should have negative revenue.
-- dbt singular tests pass when the query returns 0 rows.

select *
from {{ ref('mart_monthly_revenue') }}
where total_revenue < 0
