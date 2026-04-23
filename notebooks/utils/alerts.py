# Databricks notebook source
"""
Pipeline alerting utility.

Usage in any notebook:
    %run ./utils/alerts
    with pipeline_step("02_bronze"):
        # your notebook code here
        pass
"""

import requests
import traceback
from contextlib import contextmanager

# Webhook URL resolution order:
#   1. SLACK_WEBHOOK_URL Databricks widget (set at top of any notebook that uses alerts)
#   2. Databricks secret scope (nyc_taxi_scope / slack_webhook_url)
#   3. _WEBHOOK_URL variable below (for quick local testing only — do NOT commit a real URL)
_WEBHOOK_URL = None  # set this only for local testing, never commit a real URL

def _get_webhook_url():
    if _WEBHOOK_URL:
        return _WEBHOOK_URL
    # Try environment variable (set in test notebook or cluster env vars)
    import os
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if url:
        return url
    # Fall back to Databricks secret scope
    try:
        return dbutils.secrets.get(scope="nyc_taxi_scope", key="slack_webhook_url")
    except Exception:
        return None


def send_slack(message: str, is_error: bool = False) -> None:
    """Send a message to the configured Slack webhook. Silent if no webhook set."""
    url = _get_webhook_url()
    if not url:
        print(f"[alerts] No Slack webhook configured. Message: {message}")
        return

    icon  = ":red_circle:" if is_error else ":large_green_circle:"
    payload = {"text": f"{icon} *NYC Taxi Pipeline*\n{message}"}

    try:
        resp = requests.post(url, json=payload, timeout=5)
        resp.raise_for_status()
    except Exception as e:
        print(f"[alerts] Failed to send Slack message: {e}")


@contextmanager
def pipeline_step(step_name: str):
    """
    Context manager that wraps a notebook step with Slack alerting.

    Sends a failure alert (with traceback) if an exception is raised.
    Sends a success alert when the step completes.

    Example:
        with pipeline_step("03_silver"):
            # notebook logic here
    """
    print(f"[{step_name}] Starting...")
    try:
        yield
        msg = f"Step `{step_name}` completed successfully."
        print(f"[{step_name}] {msg}")
        send_slack(msg, is_error=False)
    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Step `{step_name}` FAILED:\n```{tb[-1500:]}```"
        print(f"[{step_name}] FAILED\n{tb}")
        send_slack(msg, is_error=True)
        raise
