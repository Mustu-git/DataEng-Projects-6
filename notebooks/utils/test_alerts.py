# Databricks notebook source
# MAGIC %md
# MAGIC # Test Slack Alerts
# MAGIC
# MAGIC Run this notebook to verify your Slack webhook is working.
# MAGIC Paste your webhook URL in the widget below, then Run All.

# COMMAND ----------

# Paste your webhook URL here to test — do NOT commit this file with a real URL
import os
os.environ["SLACK_WEBHOOK_URL"] = "YOUR_WEBHOOK_URL_HERE"

# COMMAND ----------

# MAGIC %run ./alerts

# COMMAND ----------

# Test 1: success message
send_slack("Test from NYC Taxi pipeline — alerts are working!", is_error=False)
print("Success message sent.")

# COMMAND ----------

# Test 2: error message
send_slack("This is a simulated pipeline failure alert.", is_error=True)
print("Error message sent.")

# COMMAND ----------

# Test 3: pipeline_step context manager (success path)
with pipeline_step("test_step"):
    print("Simulating pipeline step...")
    x = 1 + 1  # no error

print("Context manager test complete. Check Slack.")
