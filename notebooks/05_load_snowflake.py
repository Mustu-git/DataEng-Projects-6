# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5 — Load Gold Tables to Snowflake
# MAGIC
# MAGIC Uses snowflake-connector-python to write the three gold Delta tables to Snowflake.
# MAGIC The gold tables are small aggregations (thousands of rows), so toPandas() is fine.
# MAGIC
# MAGIC **Before running**: fill in your Snowflake credentials in section 5.1.

# COMMAND ----------

# MAGIC %pip install snowflake-connector-python pandas pyarrow

# COMMAND ----------

# MAGIC %md ## 5.1 — Credentials
# MAGIC
# MAGIC Paste your Snowflake account identifier and credentials below.
# MAGIC Do NOT commit this file to git with real credentials filled in.

# COMMAND ----------

CATALOG = spark.sql("SELECT current_catalog()").collect()[0][0]

SF_ACCOUNT   = "YOUR_ACCOUNT_IDENTIFIER"   # e.g. abc12345.us-east-1
SF_USER      = "YOUR_USERNAME"
SF_PASSWORD  = "YOUR_PASSWORD"
SF_DATABASE  = "NYC_TAXI"
SF_SCHEMA    = "GOLD"
SF_WAREHOUSE = "COMPUTE_WH"
SF_ROLE      = "ACCOUNTADMIN"

GOLD_TABLES = [
    (f"{CATALOG}.nyc_taxi.gold_daily_trips", "GOLD_DAILY_TRIPS"),
    (f"{CATALOG}.nyc_taxi.gold_zone_demand", "GOLD_ZONE_DEMAND"),
    (f"{CATALOG}.nyc_taxi.gold_peak_hours",  "GOLD_PEAK_HOURS"),
]

# COMMAND ----------

# MAGIC %md ## 5.2 — Connect to Snowflake

# COMMAND ----------

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

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
cursor.execute("USE DATABASE NYC_TAXI")
cursor.execute("USE SCHEMA GOLD")
cursor.execute("SELECT CURRENT_VERSION()")
print("Connected. Snowflake version:", cursor.fetchone()[0])

# COMMAND ----------

# MAGIC %md ## 5.3 — Write gold tables to Snowflake

# COMMAND ----------

for spark_table, sf_table in GOLD_TABLES:
    df_spark = spark.table(spark_table)
    row_count = df_spark.count()
    print(f"Loading {spark_table} ({row_count:,} rows) → {SF_DATABASE}.{SF_SCHEMA}.{sf_table} ...")

    df_pd = df_spark.toPandas()
    df_pd.columns = [c.upper() for c in df_pd.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn=conn, df=df_pd, table_name=sf_table,
        database=SF_DATABASE, schema=SF_SCHEMA,
        auto_create_table=True, overwrite=True,
    )
    print(f"  [{'OK' if success else 'FAIL'}] {nrows:,} rows written in {nchunks} chunk(s)")

print("\nAll gold tables loaded.")

# COMMAND ----------

# MAGIC %md ## 5.4 — Verify row counts

# COMMAND ----------

for spark_table, sf_table in GOLD_TABLES:
    cursor.execute(f"SELECT COUNT(*) FROM {SF_DATABASE}.{SF_SCHEMA}.{sf_table}")
    sf_count  = cursor.fetchone()[0]
    src_count = spark.table(spark_table).count()
    status    = "OK" if sf_count == src_count else "MISMATCH"
    print(f"[{status}] {sf_table}: source={src_count:,}  snowflake={sf_count:,}")

cursor.close()
conn.close()
