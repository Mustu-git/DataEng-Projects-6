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

# TLC changed column types between years (e.g. passenger_count: DOUBLE in 2019,
# INT64 in 2024). Reading with mergeSchema=True lets Spark resolve to the widest
# compatible type across all files. Schema is enforced explicitly at the write step.
raw_df = (
    spark.read
         .option("mergeSchema", "true")
         .parquet(VOL_RAW)
)
print(f"Rows read: {raw_df.count():,}")
print("Inferred merged schema:")
raw_df.printSchema()

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
