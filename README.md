# Project 6 — NYC Taxi Analytics at Scale

> PySpark · Delta Lake · Databricks · Snowflake · dbt Cloud

Processes ~50GB of NYC TLC Yellow Taxi records (2019–2024) end-to-end:
ingest → Delta Lake bronze/silver/gold → Snowflake → dbt incremental marts.

---

## Stack

| Layer | Tool |
|---|---|
| Compute | Databricks Community Edition |
| Storage | Delta Lake (DBFS) |
| Warehouse | Snowflake (30-day trial) |
| Modeling | dbt Cloud (free tier) |
| Orchestration | Databricks Workflows |

---

## Project structure

```
notebooks/
  01_ingest.py          Download TLC Parquet files; enforce explicit schema
  02_bronze.py          Write partitioned Delta bronze; baseline benchmark
  03_silver.py          Clean, derive features, broadcast join, Z-ORDER; optimized benchmark
  04_gold.py            Three gold aggregation tables
  05_load_snowflake.py  Write gold tables to Snowflake via Spark connector

dbt/
  models/staging/       Views over raw Snowflake gold tables
  models/marts/         Incremental marts (mart_monthly_revenue, mart_top_zones)
  tests/                Custom singular tests
  dbt_project.yml
  profiles.yml          Reference only — configure connection in dbt Cloud UI

workflow/
  nyc_taxi_workflow.json  Databricks Workflow: chains all 6 steps

docs/
  benchmarks.md         Record query times + dbt test results here
```

---

## Quickstart

### 1. Accounts

Create these free accounts before starting:

- **Databricks Community Edition**: https://community.cloud.databricks.com
- **Snowflake 30-day trial**: https://signup.snowflake.com
- **dbt Cloud free tier**: https://cloud.getdbt.com

### 2. Databricks setup

1. Create a cluster (Runtime 13.3 LTS, single-node is fine for CE)
2. Install Maven libraries on the cluster:
   - `net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3`
   - `net.snowflake:snowflake-jdbc:3.13.30`
3. Clone this repo into your Databricks Workspace via Repos
4. Create a Databricks Secret scope for Snowflake credentials:
   ```bash
   databricks secrets create-scope --scope nyc_taxi_scope
   databricks secrets put --scope nyc_taxi_scope --key sf_account
   databricks secrets put --scope nyc_taxi_scope --key sf_user
   databricks secrets put --scope nyc_taxi_scope --key sf_password
   ```

### 3. Run notebooks in order

```
01_ingest → 02_bronze → 03_silver → 04_gold → 05_load_snowflake
```

Record benchmark times from notebooks 02 and 03 in `docs/benchmarks.md`.

### 4. Snowflake — create database

Run this in a Snowflake worksheet before notebook 05:

```sql
CREATE DATABASE IF NOT EXISTS NYC_TAXI;
CREATE SCHEMA  IF NOT EXISTS NYC_TAXI.GOLD;
CREATE SCHEMA  IF NOT EXISTS NYC_TAXI.STAGING;
```

### 5. dbt Cloud setup

1. Connect dbt Cloud to Snowflake (Project Settings → Connection)
2. Point the repository to this repo → `dbt/` subfolder
3. Run:
   ```
   dbt deps
   dbt run --select staging marts
   dbt test
   ```

### 6. Databricks Workflow

Import `workflow/nyc_taxi_workflow.json` into Databricks Workflows:

- Workflows → Create Job → ... → Import from JSON
- Replace `<your-username>` in notebook paths with your Databricks username

---

## Benchmarks

See `docs/benchmarks.md` — fill in after running.

---

## Key design decisions

| Decision | Why |
|---|---|
| Explicit schema (no `inferSchema`) | Prevents silent type changes across TLC schema versions |
| Partition by `(year, month)` | Eliminates full-table scans for time-range queries |
| Z-ORDER on `pickup_location_id` | Co-locates zone data in files → data skipping for zone queries |
| Broadcast join for zone lookup | 265-row table — broadcasting avoids shuffling the billion-row fact table |
| Incremental dbt marts | Only new months processed on rerun → lower Snowflake credit usage |
