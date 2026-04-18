# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5 — Load Gold Tables to Snowflake
# MAGIC
# MAGIC Uses the **Snowflake Python Connector** (pip-installable) to write the three
# MAGIC gold Delta tables into Snowflake. The gold tables are small aggregations
# MAGIC (thousands of rows), so converting to pandas and using the Python connector
# MAGIC is fast and avoids the need for the Maven Spark connector.

# COMMAND ----------

# MAGIC %pip install snowflake-connector-python pandas pyarrow

# COMMAND ----------

# MAGIC %md ## 5.1 — Credentials
# MAGIC
# MAGIC Set these as environment variables in your cluster/notebook config,
# MAGIC or use Databricks Secrets if available.
# MAGIC For Community Edition, you can paste them directly here (do NOT commit to git).

# COMMAND ----------

import os

SF_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT",  "your_account_identifier")   # e.g. abc12345.us-east-1
SF_USER      = os.getenv("SNOWFLAKE_USER",      "your_username")
SF_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD",  "your_password")
SF_DATABASE  = "NYC_TAXI"
SF_SCHEMA    = "GOLD"
SF_WAREHOUSE = "COMPUTE_WH"
SF_ROLE      = "ACCOUNTADMIN"

# COMMAND ----------

# MAGIC %md ## 5.2 — Connect to Snowflake

# COMMAND ----------

import snowflake.connector

conn = snowflake.connector.connect(
    account   = SF_ACCOUNT,
    user      = SF_USER,
    password  = SF_PASSWORD,
    database  = SF_DATABASE,
    schema    = SF_SCHEMA,
    warehouse = SF_WAREHOUSE,
    role      = SF_ROLE,
)
cursor = conn.cursor()
cursor.execute("SELECT CURRENT_VERSION()")
print("Snowflake connected. Version:", cursor.fetchone()[0])

# COMMAND ----------

# MAGIC %md ## 5.3 — Helper: write a Spark DataFrame to Snowflake

# COMMAND ----------

from snowflake.connector.pandas_tools import write_pandas

def spark_to_snowflake(spark_table: str, sf_table: str):
    """Convert Spark Delta table → pandas → Snowflake table (overwrite)."""
    df_spark = spark.table(spark_table)
    row_count = df_spark.count()
    print(f"  Loading {spark_table} ({row_count:,} rows) → {SF_DATABASE}.{SF_SCHEMA}.{sf_table}")

    df_pd = df_spark.toPandas()

    # Uppercase column names — Snowflake default identifier handling
    df_pd.columns = [c.upper() for c in df_pd.columns]

    # Create or replace the table
    cursor.execute(f"DROP TABLE IF EXISTS {SF_DATABASE}.{SF_SCHEMA}.{sf_table}")

    success, nchunks, nrows, _ = write_pandas(
        conn        = conn,
        df          = df_pd,
        table_name  = sf_table,
        database    = SF_DATABASE,
        schema      = SF_SCHEMA,
        auto_create_table = True,
        overwrite   = True,
    )
    print(f"  [{'OK' if success else 'FAIL'}] {sf_table} — {nrows:,} rows written in {nchunks} chunk(s)")
    return nrows

# COMMAND ----------

# MAGIC %md ## 5.4 — Write all three gold tables

# COMMAND ----------

GOLD_TABLES = [
    ("main.nyc_taxi.gold_daily_trips", "GOLD_DAILY_TRIPS"),
    ("main.nyc_taxi.gold_zone_demand", "GOLD_ZONE_DEMAND"),
    ("main.nyc_taxi.gold_peak_hours",  "GOLD_PEAK_HOURS"),
]

for spark_table, sf_table in GOLD_TABLES:
    spark_to_snowflake(spark_table, sf_table)

print("\nAll gold tables loaded to Snowflake.")

# COMMAND ----------

# MAGIC %md ## 5.5 — Verify row counts

# COMMAND ----------

for spark_table, sf_table in GOLD_TABLES:
    cursor.execute(f"SELECT COUNT(*) FROM {SF_DATABASE}.{SF_SCHEMA}.{sf_table}")
    sf_count  = cursor.fetchone()[0]
    src_count = spark.table(spark_table).count()
    match     = "OK" if sf_count == src_count else "MISMATCH"
    print(f"[{match}] {sf_table}: source={src_count:,}  snowflake={sf_count:,}")

cursor.close()
conn.close()
