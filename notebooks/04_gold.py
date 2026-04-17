# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4 — Gold Aggregations
# MAGIC
# MAGIC Three gold tables written to Delta Lake:
# MAGIC - `gold_daily_trips`  — trips, revenue, avg fare by calendar day
# MAGIC - `gold_zone_demand`  — top pickup zones by volume across all years
# MAGIC - `gold_peak_hours`   — hourly trip patterns (hour of day × weekday/weekend)

# COMMAND ----------

# MAGIC %md ## 4.1 — Config

# COMMAND ----------

SILVER_TABLE = "nyc_taxi_silver"
GOLD_BASE    = "dbfs:/nyc_taxi/delta/gold"

# COMMAND ----------

# MAGIC %md ## 4.2 — Load Silver

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)
silver_df.cache()
print(f"Silver rows: {silver_df.count():,}")

# COMMAND ----------

# MAGIC %md ## 4.3 — gold_daily_trips

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, count, sum as spark_sum, avg,
    round as spark_round, hour
)

daily_trips_df = (
    silver_df
    .withColumn("trip_date", to_date("tpep_pickup_datetime"))
    .groupBy("trip_date")
    .agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 3).alias("avg_distance_miles"),
        spark_round(avg("trip_duration_mins"), 2).alias("avg_duration_mins"),
    )
    .orderBy("trip_date")
)

(
    daily_trips_df.write
                  .format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema", "true")
                  .save(f"{GOLD_BASE}/gold_daily_trips")
)

spark.sql("DROP TABLE IF EXISTS gold_daily_trips")
spark.sql(f"""
    CREATE TABLE gold_daily_trips
    USING DELTA LOCATION '{GOLD_BASE}/gold_daily_trips'
""")

print(f"gold_daily_trips: {daily_trips_df.count():,} rows")
daily_trips_df.show(5)

# COMMAND ----------

# MAGIC %md ## 4.4 — gold_zone_demand

# COMMAND ----------

zone_demand_df = (
    silver_df
    .groupBy("pickup_location_id", "pickup_borough", "pickup_zone")
    .agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("trip_distance"), 3).alias("avg_distance_miles"),
        spark_round(avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
    )
    .orderBy(col("total_trips").desc())
)

(
    zone_demand_df.write
                  .format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema", "true")
                  .save(f"{GOLD_BASE}/gold_zone_demand")
)

spark.sql("DROP TABLE IF EXISTS gold_zone_demand")
spark.sql(f"""
    CREATE TABLE gold_zone_demand
    USING DELTA LOCATION '{GOLD_BASE}/gold_zone_demand'
""")

print(f"gold_zone_demand: {zone_demand_df.count():,} zones")
zone_demand_df.show(10)

# COMMAND ----------

# MAGIC %md ## 4.5 — gold_peak_hours

# COMMAND ----------

from pyspark.sql.functions import when, lit

peak_hours_df = (
    silver_df
    .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    .withColumn("day_type", when(col("is_weekend"), lit("weekend")).otherwise(lit("weekday")))
    .groupBy("pickup_hour", "day_type")
    .agg(
        count("*").alias("total_trips"),
        spark_round(avg("trip_duration_mins"), 2).alias("avg_duration_mins"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
    )
    .orderBy("day_type", "pickup_hour")
)

(
    peak_hours_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .save(f"{GOLD_BASE}/gold_peak_hours")
)

spark.sql("DROP TABLE IF EXISTS gold_peak_hours")
spark.sql(f"""
    CREATE TABLE gold_peak_hours
    USING DELTA LOCATION '{GOLD_BASE}/gold_peak_hours'
""")

print(f"gold_peak_hours: {peak_hours_df.count():,} rows")
peak_hours_df.show(24)

# COMMAND ----------

# MAGIC %md ## 4.6 — Verify all three gold tables

# COMMAND ----------

for tbl in ["gold_daily_trips", "gold_zone_demand", "gold_peak_hours"]:
    cnt = spark.table(tbl).count()
    print(f"{tbl:30s} : {cnt:,} rows")
