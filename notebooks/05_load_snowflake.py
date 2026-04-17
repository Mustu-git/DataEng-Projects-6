# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5 — Load Gold Tables to Snowflake
# MAGIC
# MAGIC Uses the **Snowflake Spark Connector** to write the three gold Delta tables
# MAGIC into Snowflake.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC Before running this notebook:
# MAGIC 1. Install the Snowflake Spark connector on your Databricks cluster:
# MAGIC    - Cluster → Libraries → Install New → Maven
# MAGIC    - Coordinates: `net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3`
# MAGIC    - Also install: `net.snowflake:snowflake-jdbc:3.13.30`
# MAGIC 2. Store your Snowflake credentials in Databricks Secrets (see README.md)
# MAGIC    or set them as environment variables in the cluster config.

# COMMAND ----------

# MAGIC %md ## 5.1 — Credentials (from Databricks Secrets)
# MAGIC
# MAGIC Set up a secret scope named `nyc_taxi_scope` with the keys below:
# MAGIC ```
# MAGIC databricks secrets create-scope --scope nyc_taxi_scope
# MAGIC databricks secrets put --scope nyc_taxi_scope --key sf_account
# MAGIC databricks secrets put --scope nyc_taxi_scope --key sf_user
# MAGIC databricks secrets put --scope nyc_taxi_scope --key sf_password
# MAGIC ```

# COMMAND ----------

SF_ACCOUNT   = dbutils.secrets.get(scope="nyc_taxi_scope", key="sf_account")
SF_USER      = dbutils.secrets.get(scope="nyc_taxi_scope", key="sf_user")
SF_PASSWORD  = dbutils.secrets.get(scope="nyc_taxi_scope", key="sf_password")
SF_DATABASE  = "NYC_TAXI"
SF_SCHEMA    = "GOLD"
SF_WAREHOUSE = "COMPUTE_WH"
SF_ROLE      = "SYSADMIN"

SNOWFLAKE_OPTIONS = {
    "sfURL":       f"{SF_ACCOUNT}.snowflakecomputing.com",
    "sfUser":      SF_USER,
    "sfPassword":  SF_PASSWORD,
    "sfDatabase":  SF_DATABASE,
    "sfSchema":    SF_SCHEMA,
    "sfWarehouse": SF_WAREHOUSE,
    "sfRole":      SF_ROLE,
}

print("Snowflake options loaded (password redacted).")

# COMMAND ----------

# MAGIC %md ## 5.2 — Create Snowflake database + schema (run once)

# COMMAND ----------

# Using the Snowflake Spark connector's runQuery helper
import net.snowflake.spark.snowflake.Utils  # noqa — Scala-style import shown for reference

# In PySpark you can't import Scala objects directly; use the connector's
# `execute` approach via a Spark SQL passthrough or run DDL from a separate
# Snowflake worksheet. The SQL to run in Snowflake:
#
#   CREATE DATABASE IF NOT EXISTS NYC_TAXI;
#   CREATE SCHEMA IF NOT EXISTS NYC_TAXI.GOLD;
#
# This notebook assumes those objects already exist.

print("Ensure NYC_TAXI.GOLD database/schema exists in Snowflake before continuing.")

# COMMAND ----------

# MAGIC %md ## 5.3 — Write gold tables to Snowflake

# COMMAND ----------

GOLD_TABLES = [
    ("gold_daily_trips",  "GOLD_DAILY_TRIPS"),
    ("gold_zone_demand",  "GOLD_ZONE_DEMAND"),
    ("gold_peak_hours",   "GOLD_PEAK_HOURS"),
]

for spark_table, sf_table in GOLD_TABLES:
    df = spark.table(spark_table)
    row_count = df.count()

    print(f"Writing {spark_table} ({row_count:,} rows) → Snowflake {SF_DATABASE}.{SF_SCHEMA}.{sf_table} ...")

    (
        df.write
          .format("net.snowflake.spark.snowflake")
          .options(**SNOWFLAKE_OPTIONS)
          .option("dbtable", sf_table)
          .mode("overwrite")
          .save()
    )

    print(f"  [OK] {sf_table} written.")

print("\nAll gold tables loaded to Snowflake.")

# COMMAND ----------

# MAGIC %md ## 5.4 — Verify row counts in Snowflake

# COMMAND ----------

for spark_table, sf_table in GOLD_TABLES:
    verify_df = (
        spark.read
             .format("net.snowflake.spark.snowflake")
             .options(**SNOWFLAKE_OPTIONS)
             .option("dbtable", sf_table)
             .load()
    )
    sf_count  = verify_df.count()
    src_count = spark.table(spark_table).count()
    match     = "OK" if sf_count == src_count else "MISMATCH"
    print(f"[{match}] {sf_table}: source={src_count:,}  snowflake={sf_count:,}")
