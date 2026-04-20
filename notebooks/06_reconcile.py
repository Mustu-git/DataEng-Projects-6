# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6 — Row-Count Reconciliation
# MAGIC
# MAGIC Validates row counts across every layer of the medallion pipeline.
# MAGIC Run this after all five notebooks complete.
# MAGIC
# MAGIC | Check | Tolerance | Rationale |
# MAGIC |---|---|---|
# MAGIC | Source files → Bronze | Exact match | Casting only — no rows should be dropped |
# MAGIC | Bronze → Silver | ≥ 80 % retained | Filtering removes nulls / outliers |
# MAGIC | Silver → Gold (daily trips) | Exact match | SUM(total_trips) must equal silver count |

# COMMAND ----------

# MAGIC %md ## 6.1 — Config

# COMMAND ----------

CATALOG       = spark.sql("SELECT current_catalog()").collect()[0][0]
VOL_RAW       = f"/Volumes/{CATALOG}/nyc_taxi/nyc_taxi_vol/raw"
BRONZE_TABLE  = f"{CATALOG}.nyc_taxi.bronze"
SILVER_TABLE  = f"{CATALOG}.nyc_taxi.silver"
GOLD_DAILY    = f"{CATALOG}.nyc_taxi.gold_daily_trips"

SILVER_MIN_PCT = 0.80   # silver must retain at least 80 % of bronze rows
print(f"CATALOG={CATALOG}")

# COMMAND ----------

# MAGIC %md ## 6.2 — Count each layer

# COMMAND ----------

import os
from functools import reduce
from pyspark.sql.functions import col, lit, count, sum as spark_sum

# --- Source files (re-read headers only for counts) ---
TARGET_SCHEMA = {
    "VendorID": "long", "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp", "passenger_count": "double",
    "trip_distance": "double", "RatecodeID": "double",
    "store_and_fwd_flag": "string", "PULocationID": "long",
    "DOLocationID": "long", "payment_type": "long",
    "fare_amount": "double", "extra": "double", "mta_tax": "double",
    "tip_amount": "double", "tolls_amount": "double",
    "improvement_surcharge": "double", "total_amount": "double",
    "congestion_surcharge": "double", "airport_fee": "double",
}

files = sorted([f for f in os.listdir(VOL_RAW) if f.endswith(".parquet")])
print(f"Source files found: {len(files)}")

dfs = []
for fname in files:
    df = spark.read.parquet(f"{VOL_RAW}/{fname}")
    exprs = [
        col(c).cast(t).alias(c) if c in df.columns else lit(None).cast(t).alias(c)
        for c, t in TARGET_SCHEMA.items()
    ]
    dfs.append(df.select(exprs))

source_count = reduce(lambda a, b: a.union(b), dfs).count()
print(f"Source row count : {source_count:>15,}")

# COMMAND ----------

bronze_count = spark.table(BRONZE_TABLE).count()
print(f"Bronze row count : {bronze_count:>15,}")

# COMMAND ----------

silver_count = spark.table(SILVER_TABLE).count()
print(f"Silver row count : {silver_count:>15,}")

# COMMAND ----------

gold_trips_sum = (
    spark.table(GOLD_DAILY)
         .agg(spark_sum("total_trips").alias("total"))
         .collect()[0]["total"]
)
print(f"Gold daily SUM(total_trips) : {gold_trips_sum:>15,}")

# COMMAND ----------

# MAGIC %md ## 6.3 — Reconciliation checks

# COMMAND ----------

errors = []

# Check 1: source == bronze (exact)
if source_count != bronze_count:
    errors.append(
        f"[FAIL] Source→Bronze mismatch: source={source_count:,}  bronze={bronze_count:,}  "
        f"delta={bronze_count - source_count:+,}"
    )

# Check 2: silver >= 80 % of bronze
silver_pct = silver_count / bronze_count if bronze_count > 0 else 0
if silver_pct < SILVER_MIN_PCT:
    errors.append(
        f"[FAIL] Bronze→Silver retention {silver_pct:.1%} is below threshold {SILVER_MIN_PCT:.0%}  "
        f"(bronze={bronze_count:,}  silver={silver_count:,}  dropped={bronze_count - silver_count:,})"
    )

# Check 3: SUM(gold daily total_trips) == silver (exact)
if gold_trips_sum != silver_count:
    errors.append(
        f"[FAIL] Silver→Gold mismatch: silver={silver_count:,}  gold_sum={gold_trips_sum:,}  "
        f"delta={gold_trips_sum - silver_count:+,}"
    )

# COMMAND ----------

# MAGIC %md ## 6.4 — Summary report

# COMMAND ----------

print("=" * 60)
print("RECONCILIATION REPORT")
print("=" * 60)
print(f"  Source files          : {source_count:>15,}")
print(f"  Bronze                : {bronze_count:>15,}  {'OK  exact match' if source_count == bronze_count else 'MISMATCH'}")
print(f"  Silver                : {silver_count:>15,}  {silver_pct:.2%} of bronze  {'OK' if silver_pct >= SILVER_MIN_PCT else 'BELOW THRESHOLD'}")
print(f"  Gold daily trips sum  : {gold_trips_sum:>15,}  {'OK  exact match' if gold_trips_sum == silver_count else 'MISMATCH'}")
print("-" * 60)

if errors:
    print(f"\n{len(errors)} CHECK(S) FAILED:\n")
    for e in errors:
        print(f"  {e}")
    raise AssertionError(f"Reconciliation failed with {len(errors)} error(s). See above.")
else:
    print("\nAll reconciliation checks PASSED.")
