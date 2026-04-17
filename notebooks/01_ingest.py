# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 — Ingest NYC TLC Yellow Taxi Parquet Files (2019–2024)
# MAGIC
# MAGIC Downloads all Yellow Taxi Parquet files from the NYC TLC S3 bucket into DBFS,
# MAGIC then reads them into a single Spark DataFrame with an **explicit schema**
# MAGIC (never inferred — protects against schema drift across years).

# COMMAND ----------

# MAGIC %md ## 1.1 — Config

# COMMAND ----------

BASE_URL   = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DBFS_RAW   = "dbfs:/nyc_taxi/raw"
YEARS      = list(range(2019, 2025))   # 2019 – 2024 inclusive
MONTHS     = list(range(1, 13))

# COMMAND ----------

# MAGIC %md ## 1.2 — Download raw Parquet files to DBFS

# COMMAND ----------

import subprocess
import os

dbutils.fs.mkdirs(DBFS_RAW)

for year in YEARS:
    for month in MONTHS:
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url      = f"{BASE_URL}/{filename}"
        dbfs_dst = f"{DBFS_RAW}/{filename}"

        # Skip if already downloaded
        try:
            dbutils.fs.ls(dbfs_dst)
            print(f"  [SKIP] {filename} already exists")
            continue
        except Exception:
            pass

        # wget to /tmp, then copy to DBFS
        local_tmp = f"/tmp/{filename}"
        print(f"  [GET ] {url}")
        result = subprocess.run(
            ["wget", "-q", "-O", local_tmp, url],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  [WARN] {filename} not found (returncode={result.returncode}), skipping")
            continue

        dbutils.fs.cp(f"file:{local_tmp}", dbfs_dst)
        os.remove(local_tmp)
        print(f"  [OK  ] {filename} saved to DBFS")

print("\nAll available files downloaded.")

# COMMAND ----------

# MAGIC %md ## 1.3 — Explicit schema (never infer!)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, StringType, TimestampType
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

# MAGIC %md ## 1.4 — Read all files into a single DataFrame

# COMMAND ----------

raw_df = (
    spark.read
         .schema(YELLOW_SCHEMA)
         .parquet(DBFS_RAW)
)

row_count = raw_df.count()
print(f"Total rows ingested : {row_count:,}")
print(f"Schema              : {len(raw_df.columns)} columns")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md ## 1.5 — Quick sanity checks

# COMMAND ----------

from pyspark.sql.functions import year as spark_year, count, min as spark_min, max as spark_max

raw_df.groupBy(spark_year("tpep_pickup_datetime").alias("year")) \
      .agg(count("*").alias("row_count")) \
      .orderBy("year") \
      .show()

# COMMAND ----------

# Persist for downstream notebooks
raw_df.createOrReplaceTempView("raw_yellow_taxi")
print("TempView 'raw_yellow_taxi' registered.")
