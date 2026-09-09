"""
Redshift connection client -- same role as warehouse/snowflake_client.py,
but for Redshift. Kept as a separate module rather than a shared
abstraction over both warehouses.

Why not one generic "warehouse client" class for both: Snowflake and
Redshift diverge on exactly the operations that matter most for a
pipeline monitor -- bulk loading. Snowflake's connector.executemany()
and the file-staging COPY pattern are both reasonably fast. Redshift's
row-by-row INSERT (including executemany, which Redshift still executes
as N individual statements over the wire) is genuinely slow at volume
because Redshift is a columnar MPP store optimized for large batch
writes, not small transactional ones. The idiomatic Redshift bulk-load
path is COPY FROM an S3-staged file, not executemany -- a real
consequence of Redshift's storage engine, not just a style
preference. See redshift_loader.py for both paths.
"""

import os
from collections.abc import Generator
from contextlib import contextmanager

import redshift_connector
from redshift_connector import Connection


def _get_required_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise OSError(
            f"Missing required environment variable: {var_name}. "
            f"Check your .env file against .env.example."
        )
    return value


@contextmanager
def get_connection() -> Generator[Connection, None, None]:
    """Yield a Redshift connection, guaranteed to close on exit.

    Unlike Snowflake's auto-suspending warehouses, a Redshift cluster
    bills per hour regardless of connection count/duration -- so the
    cost argument for closing connections promptly is different here
    (connection slot exhaustion, not per-second compute billing), but
    the guaranteed-cleanup pattern is the same for the same underlying
    reason: never leak a session.
    """
    conn = redshift_connector.connect(
        host=_get_required_env("REDSHIFT_HOST"),
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        database=os.environ.get("REDSHIFT_DATABASE", "datapulse"),
        user=_get_required_env("REDSHIFT_USER"),
        password=_get_required_env("REDSHIFT_PASSWORD"),
    )
    try:
        yield conn
    finally:
        conn.close()


def test_connection() -> bool:
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            return cur.fetchone() is not None
    except Exception:  # noqa: BLE001 -- boolean health probe, any failure means "down"
        return False
