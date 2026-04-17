# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3 — Silver Transformation
# MAGIC
# MAGIC - Clean nulls / invalid values
# MAGIC - Derive `trip_duration_mins`, `fare_per_mile`, `is_weekend`
# MAGIC - **Broadcast join** for taxi zone lookup (small dimension table — avoids shuffle)
# MAGIC - Apply **Z-ORDER** on `pickup_location_id`
# MAGIC - Benchmark the same zone-level aggregation from Step 2 — record the "after" time

# COMMAND ----------

# MAGIC %md ## 3.1 — Config

# COMMAND ----------

BRONZE_PATH  = "dbfs:/nyc_taxi/delta/bronze"
SILVER_PATH  = "dbfs:/nyc_taxi/delta/silver"
SILVER_TABLE = "nyc_taxi_silver"
ZONES_URL    = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
ZONES_LOCAL  = "/tmp/taxi_zone_lookup.csv"
ZONES_DBFS   = "dbfs:/nyc_taxi/ref/taxi_zone_lookup.csv"

# COMMAND ----------

# MAGIC %md ## 3.2 — Download taxi zone lookup table

# COMMAND ----------

import subprocess

subprocess.run(["wget", "-q", "-O", ZONES_LOCAL, ZONES_URL], check=True)
dbutils.fs.mkdirs("dbfs:/nyc_taxi/ref")
dbutils.fs.cp(f"file:{ZONES_LOCAL}", ZONES_DBFS)
print("Zone lookup downloaded.")

# COMMAND ----------

# MAGIC %md ## 3.3 — Load Bronze and zone lookup

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, round as spark_round, unix_timestamp,
    dayofweek, broadcast, lit
)
from pyspark.sql.types import IntegerType

bronze_df = spark.read.format("delta").load(BRONZE_PATH)

# Zone lookup is ~265 rows — perfect for a broadcast join.
# Broadcast eliminates a shuffle: Spark sends the small table to every executor
# instead of redistributing the large fact table across the network.
zones_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(ZONES_DBFS)
         .select(
             col("LocationID").cast(IntegerType()).alias("location_id"),
             col("Borough").alias("borough"),
             col("Zone").alias("zone_name"),
             col("service_zone")
         )
)

print(f"Zone rows (should be ~265): {zones_df.count()}")
zones_df.show(5)

# COMMAND ----------

# MAGIC %md ## 3.4 — Clean + derive features

# COMMAND ----------

silver_df = (
    bronze_df
    # --- Drop rows with null keys ---
    .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                    "PULocationID", "DOLocationID"])

    # --- Filter obviously bad values ---
    .filter(col("trip_distance")  > 0)
    .filter(col("fare_amount")    > 0)
    .filter(col("passenger_count").between(1, 8))
    .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))

    # --- Cast & rename ---
    .withColumn("pickup_location_id",  col("PULocationID").cast(IntegerType()))
    .withColumn("dropoff_location_id", col("DOLocationID").cast(IntegerType()))

    # --- Derived features ---
    .withColumn(
        "trip_duration_mins",
        spark_round(
            (unix_timestamp("tpep_dropoff_datetime") -
             unix_timestamp("tpep_pickup_datetime")) / 60.0,
            2
        )
    )
    .withColumn(
        "fare_per_mile",
        spark_round(
            when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance"))
            .otherwise(lit(None)),
            2
        )
    )
    .withColumn(
        "is_weekend",
        when(dayofweek("tpep_pickup_datetime").isin(1, 7), True).otherwise(False)
    )

    # --- Filter implausible durations ---
    .filter(col("trip_duration_mins").between(1, 240))
    .filter(col("fare_per_mile") < 100)

    .select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
        "trip_duration_mins",
        "fare_per_mile",
        "is_weekend",
        "year",
        "month",
    )
)

# COMMAND ----------

# MAGIC %md ## 3.5 — Broadcast join with zone lookup
# MAGIC
# MAGIC `broadcast(zones_df)` hints Spark to send the 265-row lookup table to each
# MAGIC executor in-memory, avoiding a shuffle of the multi-billion-row fact table.
# MAGIC This is the right call when one side of a join is < ~10MB.

# COMMAND ----------

silver_with_zones = (
    silver_df
    .join(
        broadcast(zones_df.select(
            col("location_id"),
            col("borough").alias("pickup_borough"),
            col("zone_name").alias("pickup_zone")
        )),
        silver_df.pickup_location_id == col("location_id"),
        how="left"
    )
    .drop("location_id")
)

# COMMAND ----------

# MAGIC %md ## 3.6 — Write Silver Delta table

# COMMAND ----------

(
    silver_with_zones.write
                     .format("delta")
                     .mode("overwrite")
                     .partitionBy("year", "month")
                     .option("overwriteSchema", "true")
                     .save(SILVER_PATH)
)

spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
spark.sql(f"""
    CREATE TABLE {SILVER_TABLE}
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print(f"Silver table written. Rows: {spark.read.format('delta').load(SILVER_PATH).count():,}")

# COMMAND ----------

# MAGIC %md ## 3.7 — Apply Z-ORDER on pickup_location_id
# MAGIC
# MAGIC Z-ORDER co-locates data with the same `pickup_location_id` in the same Delta
# MAGIC files. When the zone-level aggregation runs, Spark can skip irrelevant files
# MAGIC entirely (data skipping), dramatically reducing I/O.

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {SILVER_TABLE}
    ZORDER BY (pickup_location_id)
""")
print("Z-ORDER applied on pickup_location_id.")

# COMMAND ----------

# MAGIC %md ## 3.8 — POST-OPTIMIZATION BENCHMARK
# MAGIC
# MAGIC Same query as in notebook 02. Record the new time and compute the speedup.

# COMMAND ----------

import time

spark.catalog.clearCache()

query = """
    SELECT
        pickup_location_id    AS pickup_zone,
        COUNT(*)              AS trip_count,
        ROUND(SUM(total_amount), 2)  AS total_revenue,
        ROUND(AVG(trip_distance), 3) AS avg_distance
    FROM nyc_taxi_silver
    GROUP BY pickup_location_id
    ORDER BY trip_count DESC
    LIMIT 20
"""

t0 = time.time()
result_df = spark.sql(query)
result_df.show()
t1 = time.time()

optimized_secs = round(t1 - t0, 2)
print(f"\n>>> OPTIMIZED query time (Z-ORDER + partitioning): {optimized_secs}s")
print("  -> Record this in docs/benchmarks.md and compute speedup ratio")

# COMMAND ----------

# Quick schema check
silver_with_zones.printSchema()
