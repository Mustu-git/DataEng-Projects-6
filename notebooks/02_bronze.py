# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2 — Delta Lake Bronze Layer
# MAGIC
# MAGIC Reads raw Parquet files with per-file casting (handles TLC schema evolution),
# MAGIC writes a partitioned Delta table, and records the **baseline benchmark**.

# COMMAND ----------

# MAGIC %md ## 2.1 — Config

# COMMAND ----------

import os
from functools import reduce
from pyspark.sql.functions import col, lit, year, month

CATALOG      = spark.sql("SELECT current_catalog()").collect()[0][0]
VOL_RAW      = f"/Volumes/{CATALOG}/nyc_taxi/nyc_taxi_vol/raw"
BRONZE_TABLE = f"{CATALOG}.nyc_taxi.bronze"
print(f"CATALOG={CATALOG}   BRONZE_TABLE={BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## 2.2 — Read raw files with unified schema
# MAGIC
# MAGIC TLC changed some columns from DOUBLE to BIGINT in 2024. We read each file
# MAGIC individually and cast every column to a fixed target schema before union.

# COMMAND ----------

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
print(f"Files to process: {len(files)}")

dfs = []
for fname in files:
    df = spark.read.parquet(f"{VOL_RAW}/{fname}")
    exprs = [
        col(c).cast(t).alias(c) if c in df.columns else lit(None).cast(t).alias(c)
        for c, t in TARGET_SCHEMA.items()
    ]
    dfs.append(df.select(exprs))

raw_df = reduce(lambda a, b: a.union(b), dfs)
print(f"Total rows: {raw_df.count():,}")

# COMMAND ----------

# MAGIC %md ## 2.3 — Write Bronze Delta table (partitioned by year, month)

# COMMAND ----------

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

print(f"Bronze table '{BRONZE_TABLE}' written, partitioned by (year, month).")

# COMMAND ----------

# MAGIC %md ## 2.4 — BASELINE BENCHMARK
# MAGIC
# MAGIC Zone-level aggregation with no optimization. Record this time in benchmarks.md.

# COMMAND ----------

import time

spark.catalog.clearCache()

t0 = time.time()
spark.sql(f"""
    SELECT PULocationID AS pickup_zone,
           COUNT(*) AS trip_count,
           ROUND(SUM(total_amount), 2) AS total_revenue,
           ROUND(AVG(trip_distance), 3) AS avg_distance
    FROM {BRONZE_TABLE}
    GROUP BY PULocationID
    ORDER BY trip_count DESC
    LIMIT 20
""").show()
t1 = time.time()

print(f"\n>>> BASELINE query time: {round(t1-t0, 2)}s  — record in docs/benchmarks.md")
