# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2 — Delta Lake Bronze Layer
# MAGIC
# MAGIC Writes raw data to a Unity Catalog managed Delta table partitioned by
# MAGIC (year, month), then benchmarks a zone-level aggregation **before** any
# MAGIC optimization. Record the result in `docs/benchmarks.md`.

# COMMAND ----------

# MAGIC %md ## 2.1 — Config

# COMMAND ----------

CATALOG      = spark.sql("SELECT current_catalog()").collect()[0][0]
VOL_RAW      = f"/Volumes/{CATALOG}/nyc_taxi/nyc_taxi_vol/raw"
BRONZE_TABLE = f"{CATALOG}.nyc_taxi.bronze"
print(f"CATALOG={CATALOG}  BRONZE_TABLE={BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## 2.2 — Load raw Parquet files

# COMMAND ----------

import os
from functools import reduce
from pyspark.sql.functions import col, lit

# TLC changed column types between years (DOUBLE in 2019-2023, BIGINT in 2024+).
# mergeSchema can't resolve type conflicts, so we read each file individually,
# cast every column to a fixed target schema, then union all DataFrames.
TARGET_SCHEMA = {
    "VendorID":              "long",
    "tpep_pickup_datetime":  "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count":       "double",
    "trip_distance":         "double",
    "RatecodeID":            "double",
    "store_and_fwd_flag":    "string",
    "PULocationID":          "long",
    "DOLocationID":          "long",
    "payment_type":          "long",
    "fare_amount":           "double",
    "extra":                 "double",
    "mta_tax":               "double",
    "tip_amount":            "double",
    "tolls_amount":          "double",
    "improvement_surcharge": "double",
    "total_amount":          "double",
    "congestion_surcharge":  "double",
    "airport_fee":           "double",
}

files = sorted([f for f in os.listdir(VOL_RAW) if f.endswith(".parquet")])
print(f"Found {len(files)} parquet files")

dfs = []
for fname in files:
    df = spark.read.parquet(f"{VOL_RAW}/{fname}")
    select_exprs = [
        col(c).cast(t).alias(c) if c in df.columns else lit(None).cast(t).alias(c)
        for c, t in TARGET_SCHEMA.items()
    ]
    dfs.append(df.select(select_exprs))

raw_df = reduce(lambda a, b: a.union(b), dfs)
print(f"Rows read: {raw_df.count():,}")

# COMMAND ----------

# MAGIC %md ## 2.3 — Add partition columns and write Bronze Delta table

# COMMAND ----------

from pyspark.sql.functions import year, month

bronze_df = (
    raw_df
    .withColumn("year",  year("tpep_pickup_datetime").cast("int"))
    .withColumn("month", month("tpep_pickup_datetime").cast("int"))
    .filter("year BETWEEN 2019 AND 2024")
    .filter("month BETWEEN 1 AND 12")
)

(
    bronze_df.write
             .format("delta")
             .mode("overwrite")
             .partitionBy("year", "month")
             .option("overwriteSchema", "true")
             .saveAsTable(BRONZE_TABLE)
)

print(f"Bronze table '{BRONZE_TABLE}' written and partitioned by (year, month).")

# COMMAND ----------

# MAGIC %md ## 2.4 — BASELINE BENCHMARK (before optimization)

# COMMAND ----------

import time

spark.catalog.clearCache()

query = f"""
    SELECT
        PULocationID          AS pickup_zone,
        COUNT(*)              AS trip_count,
        ROUND(SUM(total_amount), 2)  AS total_revenue,
        ROUND(AVG(trip_distance), 3) AS avg_distance
    FROM {BRONZE_TABLE}
    GROUP BY PULocationID
    ORDER BY trip_count DESC
    LIMIT 20
"""

t0 = time.time()
result_df = spark.sql(query)
result_df.show()
t1 = time.time()

baseline_secs = round(t1 - t0, 2)
print(f"\n>>> BASELINE query time (no optimization): {baseline_secs}s")
print("  -> Record this in docs/benchmarks.md")

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL {BRONZE_TABLE}").select("numFiles", "sizeInBytes").show(truncate=False)
