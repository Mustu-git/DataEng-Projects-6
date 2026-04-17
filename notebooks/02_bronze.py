# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2 — Delta Lake Bronze Layer
# MAGIC
# MAGIC Writes raw data to a partitioned Delta table and benchmarks a zone-level
# MAGIC aggregation **before** any optimization (Z-ORDER / caching).
# MAGIC Record the benchmark result in `docs/benchmarks.md`.

# COMMAND ----------

# MAGIC %md ## 2.1 — Config

# COMMAND ----------

BRONZE_PATH = "dbfs:/nyc_taxi/delta/bronze"
BRONZE_TABLE = "nyc_taxi_bronze"

# COMMAND ----------

# MAGIC %md ## 2.2 — Load raw data (from notebook 01 or direct read)

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

raw_df = spark.read.schema(YELLOW_SCHEMA).parquet("dbfs:/nyc_taxi/raw")
print(f"Rows: {raw_df.count():,}")

# COMMAND ----------

# MAGIC %md ## 2.3 — Add partition columns and write Bronze Delta table

# COMMAND ----------

from pyspark.sql.functions import year, month

bronze_df = (
    raw_df
    .withColumn("year",  year("tpep_pickup_datetime").cast("int"))
    .withColumn("month", month("tpep_pickup_datetime").cast("int"))
    # Filter to valid years only — TLC data occasionally has erroneous future dates
    .filter("year BETWEEN 2019 AND 2024")
    .filter("month BETWEEN 1 AND 12")
)

(
    bronze_df.write
             .format("delta")
             .mode("overwrite")
             .partitionBy("year", "month")
             .option("overwriteSchema", "true")
             .save(BRONZE_PATH)
)

print(f"Bronze Delta table written to {BRONZE_PATH}")
print(f"Partitioned by: (year, month)")

# COMMAND ----------

# Register as SQL table for easy querying
spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
spark.sql(f"""
    CREATE TABLE {BRONZE_TABLE}
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")
print(f"Table '{BRONZE_TABLE}' registered.")

# COMMAND ----------

# MAGIC %md ## 2.4 — BASELINE BENCHMARK (before optimization)
# MAGIC
# MAGIC Run the zone-level aggregation and **record the elapsed time**.
# MAGIC This is the "before" number for your resume bullet.

# COMMAND ----------

import time

# Clear Spark cache to get a cold-read benchmark
spark.catalog.clearCache()

query = """
    SELECT
        PULocationID          AS pickup_zone,
        COUNT(*)              AS trip_count,
        ROUND(SUM(total_amount), 2)  AS total_revenue,
        ROUND(AVG(trip_distance), 3) AS avg_distance
    FROM nyc_taxi_bronze
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

# MAGIC %md ## 2.5 — Delta table stats

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL {BRONZE_TABLE}").select(
    "numFiles", "sizeInBytes"
).show(truncate=False)
