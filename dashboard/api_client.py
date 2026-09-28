"""HTTP client for the DataPulse API. The dashboard never talks to Snowflake.

Why: one access path to the data (the API reads only the tested dbt marts),
and Snowflake credentials live only in the API container -- the dashboard
container has no .env at all.
"""

import os

import requests

API_URL = os.environ.get("API_URL", "http://localhost:8010")
TIMEOUT_SECONDS = 15


def get(path: str, **params) -> list | dict:
    response = requests.get(f"{API_URL}{path}", params=params, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()
