"""
Concept test for ingestion/scheduler.py's polling loop.

Runs the REAL poll_forever() coroutine through asyncio.run(), with
run_ingestion_cycle and asyncio.sleep mocked out -- proves the loop
actually calls the ingestion cycle and then waits, without making a
real network call or actually sleeping 60 seconds in the test suite.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from ingestion.scheduler import poll_forever


class _StopLoop(Exception):
    """Raised by the mocked sleep to escape the otherwise-infinite loop."""


def test_poll_forever_runs_a_cycle_then_sleeps_before_looping():
    """
    I learned: asyncio.to_thread(sync_func, *args) runs a blocking
    sync function (run_ingestion_cycle -- built on the sync
    requests/snowflake-connector calls from Day 1) in a worker thread,
    so `await`ing it doesn't freeze the event loop. This test proves
    the loop's actual sequence: call the cycle, THEN sleep, THEN
    repeat -- by making the mocked sleep raise to escape the loop
    right after the first iteration completes.
    """
    with (
        patch("ingestion.scheduler.run_ingestion_cycle") as mock_cycle,
        patch("ingestion.scheduler.asyncio.sleep", new=AsyncMock(side_effect=_StopLoop)),
    ):
        mock_cycle.return_value = {"status": "success", "rows_ingested": 42}

        with pytest.raises(_StopLoop):
            asyncio.run(poll_forever())

        mock_cycle.assert_called_once()


def test_a_failed_cycle_does_not_crash_the_loop():
    """
    I learned: the try/except inside poll_forever wraps ONLY the
    ingestion cycle call, not the sleep -- so a raised exception from
    a bad cycle is caught and logged, and the loop still reaches
    asyncio.sleep() and would continue polling. Without this, one
    transient network failure would kill the whole long-running
    container instead of just failing that one cycle.
    """
    with (
        patch("ingestion.scheduler.run_ingestion_cycle", side_effect=RuntimeError("boom")),
        patch("ingestion.scheduler.asyncio.sleep", new=AsyncMock(side_effect=_StopLoop)),
        pytest.raises(_StopLoop),
    ):
        # If the exception from run_ingestion_cycle propagated out of
        # poll_forever uncaught, we'd see RuntimeError here instead of
        # _StopLoop -- reaching _StopLoop proves the loop survived the
        # failure and got as far as calling sleep().
        asyncio.run(poll_forever())
