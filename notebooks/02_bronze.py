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

VOL_RAW      = "/Volumes/main/nyc_taxi/nyc_taxi_vol/raw"
BRONZE_TABLE = "main.nyc_taxi.bronze"

# COMMAND ----------

# MAGIC %md ## 2.2 — Load raw Parquet files

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

raw_df = spark.read.schema(YELLOW_SCHEMA).parquet(VOL_RAW)
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
