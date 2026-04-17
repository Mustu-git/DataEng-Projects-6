# Benchmark Results — NYC Taxi at Scale

Fill in the values below after running notebooks 02 and 03.

## Dataset

| Metric | Value |
|---|---|
| Years covered | 2019 – 2024 |
| Total rows (after filtering) | _fill in_ |
| Raw file size (GB) | ~50 GB |

## Query: Zone-level aggregation (top 20 pickup zones by trip count)

```sql
SELECT pickup_location_id, COUNT(*), SUM(total_amount), AVG(trip_distance)
FROM <table>
GROUP BY pickup_location_id
ORDER BY COUNT(*) DESC
LIMIT 20
```

| Stage | Table | Time (seconds) | Notes |
|---|---|---|---|
| Baseline | `nyc_taxi_bronze` | _fill in_ | Raw Delta, no optimization |
| After Z-ORDER | `nyc_taxi_silver` | _fill in_ | + partitioning + Z-ORDER on pickup_location_id |

**Speedup factor**: _baseline / optimized_ = **Xx**

## dbt Tests

| Test | Status |
|---|---|
| mart_monthly_revenue.month_start_date — not_null | ✅ |
| mart_monthly_revenue.month_start_date — unique | ✅ |
| mart_monthly_revenue.trip_year — accepted_values | ✅ |
| mart_monthly_revenue.trip_month — accepted_values | ✅ |
| mart_monthly_revenue.total_trips — not_null | ✅ |
| mart_monthly_revenue.total_revenue — not_null | ✅ |
| mart_top_zones.pickup_location_id — not_null | ✅ |
| mart_top_zones.pickup_location_id — unique | ✅ |
| mart_top_zones.pickup_borough — accepted_values | ✅ |
| mart_top_zones.total_trips — not_null | ✅ |
| mart_top_zones.overall_rank — not_null | ✅ |
| assert_no_negative_revenue | ✅ |
| assert_daily_trips_positive | ✅ |

**Total passing**: _X_ / 13

## Resume Bullet (fill in after benchmarking)

```
Processed 6 years of NYC TLC trip records (~50GB, XM rows) with PySpark on Databricks;
partitioned Delta tables by (year, month) with Z-ORDER on pickup_location_id —
reducing zone-level query time from Xs to Xs (Xx speedup).
Used broadcast join for zone lookup to eliminate shuffle on a 265-row dimension table.
Loaded gold aggregations into Snowflake via the Spark connector; built incremental dbt
marts with 13 schema tests, reducing warehouse compute on reruns by only processing
new months.
```
