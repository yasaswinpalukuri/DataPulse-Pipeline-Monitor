"""
Snowflake connection client.

Design decision: this module is the ONLY place that imports
`snowflake.connector` directly. Every other module in the project
calls `get_connection()` from here. Reasons:

1. Credentials and connection params (account, warehouse, role) live
   in exactly one place, sourced from environment variables. Rotating
   a password or switching warehouses is a one-file change.
2. It's the seam for later production hardening (connection pooling,
   retry/backoff) without touching business logic elsewhere.
3. `DictCursor` is configured here once, so every query in the codebase
   gets dict-shaped rows (`row["fare_amount"]`) instead of the
   connector's default tuple rows (`row[3]`), which is a common
   source of bugs when column order in a SELECT changes.
"""

import os
from collections.abc import Generator
from contextlib import contextmanager

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor


def _get_required_env(var_name: str) -> str:
    """Fail loudly and immediately if a required Snowflake env var is missing.

    Why fail here instead of letting the connector raise its own error:
    the connector's error for a missing account identifier is a generic
    auth failure that doesn't tell you WHICH env var is unset. This gives
    a precise, actionable error before we even attempt a network call.
    """
    value = os.environ.get(var_name)
    if not value:
        raise OSError(
            f"Missing required environment variable: {var_name}. "
            f"Check your .env file against .env.example."
        )
    return value


@contextmanager
def get_connection() -> Generator[SnowflakeConnection, None, None]:
    """Yield a Snowflake connection, guaranteed to close on exit.

    Usage:
        with get_connection() as conn:
            cur = conn.cursor(DictCursor)
            cur.execute("SELECT 1")
            rows = cur.fetchall()

    Why a context manager and not a module-level singleton connection:
    Snowflake bills warehouse compute by the second while a session is
    active (with a minimum billing window). A long-lived global
    connection sitting idle between ingestion runs quietly burns
    credits. Opening/closing per unit of work keeps the warehouse
    auto-suspending between runs, which is the cost-correct pattern
    for a polling-style ingestion worker like this one.
    """
    conn = snowflake.connector.connect(
        account=_get_required_env("SNOWFLAKE_ACCOUNT"),
        user=_get_required_env("SNOWFLAKE_USER"),
        password=_get_required_env("SNOWFLAKE_PASSWORD"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "DATAPULSE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    try:
        yield conn
    finally:
        conn.close()


def run_query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a query and return rows as a list of dicts.

    This is the function most of the rest of the codebase should call
    rather than reaching for get_connection() directly — it centralizes
    the DictCursor choice described above.
    """
    with get_connection() as conn:
        cur = conn.cursor(DictCursor)
        cur.execute(sql, params)
        return cur.fetchall()


def execute(sql: str, params: tuple = ()) -> int:
    """Execute a statement that doesn't return rows (INSERT/CREATE/etc).

    Returns rowcount so callers (e.g. the loader) can confirm how many
    rows were actually written, rather than assuming success silently.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.rowcount


def test_connection() -> bool:
    """Quick sanity check used by /health in FastAPI and manual smoke tests."""
    try:
        result = run_query("SELECT CURRENT_VERSION()")
        return len(result) == 1
    except Exception:  # noqa: BLE001 -- this is a boolean health probe;
        # /health should report "down" for ANY failure mode (auth,
        # network, warehouse suspended), not just ones we anticipated.
        return False
