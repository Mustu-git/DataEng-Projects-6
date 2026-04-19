# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 — Ingest NYC TLC Yellow Taxi Parquet Files (2019–2024)
# MAGIC
# MAGIC Downloads all Yellow Taxi Parquet files from the NYC TLC CDN into a
# MAGIC Unity Catalog Volume, then validates row counts by year.

# COMMAND ----------

# MAGIC %md ## 1.0 — One-time setup: create UC schema + volume

# COMMAND ----------

CATALOG = spark.sql("SELECT current_catalog()").collect()[0][0]
print(f"Catalog: {CATALOG}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.nyc_taxi")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.nyc_taxi.nyc_taxi_vol")

VOL_BASE = f"/Volumes/{CATALOG}/nyc_taxi/nyc_taxi_vol"
VOL_RAW  = f"{VOL_BASE}/raw"

import os
os.makedirs(VOL_RAW, exist_ok=True)
print(f"Volume path: {VOL_BASE}")

# COMMAND ----------

# MAGIC %md ## 1.1 — Download raw Parquet files

# COMMAND ----------

import subprocess

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEARS    = list(range(2019, 2025))
MONTHS   = list(range(1, 13))

for year in YEARS:
    for month in MONTHS:
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        dst      = f"{VOL_RAW}/{filename}"

        if os.path.exists(dst) and os.path.getsize(dst) > 10_000:
            print(f"  [SKIP] {filename}")
            continue

        url = f"{BASE_URL}/{filename}"
        print(f"  [GET ] {url}")
        r = subprocess.run(["wget", "-q", "-O", dst, url], capture_output=True, text=True)

        if r.returncode != 0 or not os.path.exists(dst) or os.path.getsize(dst) < 10_000:
            print(f"  [WARN] {filename} not found or too small, removing")
            if os.path.exists(dst):
                os.remove(dst)
            continue

        print(f"  [OK  ] {filename} ({os.path.getsize(dst)/1e6:.1f} MB)")

print("\nDownload complete.")

# COMMAND ----------

# MAGIC %md ## 1.2 — Validate: row counts by year

# COMMAND ----------

from functools import reduce
from pyspark.sql.functions import col, lit, year as spark_year, count

# Read each file individually to avoid schema conflicts between years
# (TLC changed some columns from DOUBLE to BIGINT in 2024+)
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
print(f"Files found: {len(files)}")

dfs = []
for fname in files:
    df = spark.read.parquet(f"{VOL_RAW}/{fname}")
    exprs = [
        col(c).cast(t).alias(c) if c in df.columns else lit(None).cast(t).alias(c)
        for c, t in TARGET_SCHEMA.items()
    ]
    dfs.append(df.select(exprs))

raw_df = reduce(lambda a, b: a.union(b), dfs)

raw_df.groupBy(spark_year("tpep_pickup_datetime").alias("year")) \
      .agg(count("*").alias("row_count")) \
      .orderBy("year").show()

print(f"Total rows: {raw_df.count():,}")
print("Notebook 01 complete — proceed to 02_bronze")
