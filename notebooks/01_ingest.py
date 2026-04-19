# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 — Ingest NYC TLC Yellow Taxi Parquet Files (2019–2024)

# COMMAND ----------

# MAGIC %md ## 1.0 — Detect catalog and create schema + volume

# COMMAND ----------

# Detect the actual current catalog (differs by workspace)
CATALOG = spark.sql("SELECT current_catalog()").collect()[0][0]
print(f"Current catalog: {CATALOG}")

spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.nyc_taxi")
print(f"Schema {CATALOG}.nyc_taxi ready.")

# Create a managed volume for raw file storage
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.nyc_taxi.nyc_taxi_vol")
VOL_BASE = f"/Volumes/{CATALOG}/nyc_taxi/nyc_taxi_vol"
print(f"Volume path: {VOL_BASE}")

# COMMAND ----------

# MAGIC %md ## 1.1 — Config

# COMMAND ----------

import os

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
VOL_RAW  = f"{VOL_BASE}/raw"
YEARS    = list(range(2019, 2025))
MONTHS   = list(range(1, 13))

os.makedirs(VOL_RAW, exist_ok=True)
print(f"Raw files destination: {VOL_RAW}")

# COMMAND ----------

# MAGIC %md ## 1.2 — Download raw Parquet files to Volume

# COMMAND ----------

import subprocess

for year in YEARS:
    for month in MONTHS:
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        dst_path = f"{VOL_RAW}/{filename}"

        if os.path.exists(dst_path) and os.path.getsize(dst_path) > 1000:
            print(f"  [SKIP] {filename}")
            continue

        url = f"{BASE_URL}/{filename}"
        print(f"  [GET ] {url}")
        result = subprocess.run(["wget", "-q", "-O", dst_path, url],
                                capture_output=True, text=True)
        if result.returncode != 0 or not os.path.exists(dst_path) or os.path.getsize(dst_path) < 1000:
            print(f"  [WARN] {filename} not found or empty, skipping")
            if os.path.exists(dst_path):
                os.remove(dst_path)
            continue

        size_mb = os.path.getsize(dst_path) / 1_000_000
        print(f"  [OK  ] {filename} ({size_mb:.1f} MB)")

print("\nAll available files downloaded.")

# COMMAND ----------

# MAGIC %md ## 1.3 — Explicit schema

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, StringType, TimestampType
)

YELLOW_SCHEMA = StructType([
    StructField("VendorID",                LongType(),      True),
    StructField("tpep_pickup_datetime",    TimestampType(), True),
    StructField("tpep_dropoff_datetime",   TimestampType(), True),
    StructField("passenger_count",         DoubleType(),    True),
    StructField("trip_distance",           DoubleType(),    True),
    StructField("RatecodeID",              DoubleType(),    True),
    StructField("store_and_fwd_flag",      StringType(),    True),
    StructField("PULocationID",            LongType(),      True),
    StructField("DOLocationID",            LongType(),      True),
    StructField("payment_type",            LongType(),      True),
    StructField("fare_amount",             DoubleType(),    True),
    StructField("extra",                   DoubleType(),    True),
    StructField("mta_tax",                 DoubleType(),    True),
    StructField("tip_amount",              DoubleType(),    True),
    StructField("tolls_amount",            DoubleType(),    True),
    StructField("improvement_surcharge",   DoubleType(),    True),
    StructField("total_amount",            DoubleType(),    True),
    StructField("congestion_surcharge",    DoubleType(),    True),
    StructField("airport_fee",             DoubleType(),    True),
])

# COMMAND ----------

# MAGIC %md ## 1.4 — Read all files

# COMMAND ----------

raw_df = spark.read.schema(YELLOW_SCHEMA).parquet(VOL_RAW)

row_count = raw_df.count()
print(f"Total rows ingested : {row_count:,}")
raw_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import year as spark_year, count

raw_df.groupBy(spark_year("tpep_pickup_datetime").alias("year")) \
      .agg(count("*").alias("row_count")) \
      .orderBy("year").show()

# COMMAND ----------

raw_df.createOrReplaceTempView("raw_yellow_taxi")
print(f"Done. CATALOG={CATALOG}  VOL_BASE={VOL_BASE}")
