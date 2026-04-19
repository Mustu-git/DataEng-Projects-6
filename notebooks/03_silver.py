# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3 — Silver Transformation
# MAGIC
# MAGIC - Clean nulls and invalid values
# MAGIC - Derive `trip_duration_mins`, `fare_per_mile`, `is_weekend`
# MAGIC - Broadcast join with taxi zone lookup (265 rows — eliminates shuffle)
# MAGIC - Apply Z-ORDER on `pickup_location_id`
# MAGIC - Record the post-optimization benchmark

# COMMAND ----------

# MAGIC %md ## 3.1 — Config

# COMMAND ----------

import subprocess, os

CATALOG      = spark.sql("SELECT current_catalog()").collect()[0][0]
BRONZE_TABLE = f"{CATALOG}.nyc_taxi.bronze"
SILVER_TABLE = f"{CATALOG}.nyc_taxi.silver"
ZONES_URL    = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
ZONES_LOCAL  = f"/Volumes/{CATALOG}/nyc_taxi/nyc_taxi_vol/taxi_zone_lookup.csv"
print(f"CATALOG={CATALOG}")

# COMMAND ----------

# MAGIC %md ## 3.2 — Download taxi zone lookup

# COMMAND ----------

r = subprocess.run(["wget", "-q", "-O", ZONES_LOCAL, ZONES_URL], capture_output=True, text=True)
print("Zone lookup ready." if r.returncode == 0 else f"wget error: {r.stderr}")

# COMMAND ----------

# MAGIC %md ## 3.3 — Load bronze + zone lookup

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, round as spark_round, unix_timestamp,
    dayofweek, broadcast, lit
)
from pyspark.sql.types import IntegerType

bronze_df = spark.table(BRONZE_TABLE)

# Zone lookup is ~265 rows. Broadcast hint sends it to every executor in-memory,
# avoiding a shuffle of the 250M+ row fact table across the network.
zones_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(ZONES_LOCAL)
         .select(
             col("LocationID").cast(IntegerType()).alias("location_id"),
             col("Borough").alias("borough"),
             col("Zone").alias("zone_name"),
         )
)
print(f"Zone rows: {zones_df.count()}")

# COMMAND ----------

# MAGIC %md ## 3.4 — Clean + derive features

# COMMAND ----------

silver_df = (
    bronze_df
    .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                    "PULocationID", "DOLocationID"])
    .filter(col("trip_distance")  > 0)
    .filter(col("fare_amount")    > 0)
    .filter(col("passenger_count").between(1, 8))
    .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    .withColumn("pickup_location_id",  col("PULocationID").cast(IntegerType()))
    .withColumn("dropoff_location_id", col("DOLocationID").cast(IntegerType()))
    .withColumn(
        "trip_duration_mins",
        spark_round(
            (unix_timestamp("tpep_dropoff_datetime") -
             unix_timestamp("tpep_pickup_datetime")) / 60.0, 2
        )
    )
    .withColumn(
        "fare_per_mile",
        spark_round(
            when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance"))
            .otherwise(lit(None)), 2
        )
    )
    .withColumn(
        "is_weekend",
        when(dayofweek("tpep_pickup_datetime").isin(1, 7), True).otherwise(False)
    )
    .filter(col("trip_duration_mins").between(1, 240))
    .filter(col("fare_per_mile") < 100)
    .select(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "pickup_location_id", "dropoff_location_id", "passenger_count",
        "trip_distance", "fare_amount", "tip_amount", "total_amount",
        "payment_type", "trip_duration_mins", "fare_per_mile",
        "is_weekend", "year", "month",
    )
)

# COMMAND ----------

# MAGIC %md ## 3.5 — Broadcast join with zone lookup

# COMMAND ----------

silver_with_zones = (
    silver_df
    .join(
        broadcast(zones_df.select(
            col("location_id"),
            col("borough").alias("pickup_borough"),
            col("zone_name").alias("pickup_zone"),
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
                     .saveAsTable(SILVER_TABLE)
)
print(f"Silver table '{SILVER_TABLE}' written.")

# COMMAND ----------

# MAGIC %md ## 3.7 — Z-ORDER on pickup_location_id
# MAGIC
# MAGIC Co-locates rows with the same pickup_location_id in the same Delta files.
# MAGIC Enables file-level data skipping on zone queries, dramatically reducing I/O.

# COMMAND ----------

spark.sql(f"OPTIMIZE {SILVER_TABLE} ZORDER BY (pickup_location_id)")
print("Z-ORDER applied.")

# COMMAND ----------

# MAGIC %md ## 3.8 — POST-OPTIMIZATION BENCHMARK

# COMMAND ----------

import time

spark.catalog.clearCache()

t0 = time.time()
spark.sql(f"""
    SELECT pickup_location_id AS pickup_zone,
           COUNT(*) AS trip_count,
           ROUND(SUM(total_amount), 2) AS total_revenue,
           ROUND(AVG(trip_distance), 3) AS avg_distance
    FROM {SILVER_TABLE}
    GROUP BY pickup_location_id
    ORDER BY trip_count DESC
    LIMIT 20
""").show()
t1 = time.time()

print(f"\n>>> OPTIMIZED query time: {round(t1-t0, 2)}s  — record in docs/benchmarks.md")
