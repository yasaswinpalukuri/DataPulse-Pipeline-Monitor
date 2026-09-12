"""
Continuous polling loop around ingestion.run_once.run_ingestion_cycle.

Why asyncio and not a cron job calling run_once.py repeatedly: cron
would spin up a fresh Python interpreter and a fresh Snowflake
connection every 60 seconds -- wasteful at that cadence, and it
doesn't fit Docker's model (a container supervised by `restart:
unless-stopped` expects ONE long-running foreground process, not a
disjoint sequence of short-lived ones). A single asyncio loop keeps
one process alive and sleeps between cycles instead.

Why asyncio.to_thread() around a synchronous function: run_ingestion_cycle
uses the blocking requests + snowflake-connector calls from Day 1 --
neither is async-native. Wrapping the whole sync call in
asyncio.to_thread() runs it in a worker thread so the event loop isn't
frozen for the ~seconds it takes to complete, which matters the moment
a second concurrent task (e.g. the Day 7 S3 poller) gets added to this
same loop.
"""

import asyncio
import os

from ingestion.run_once import run_ingestion_cycle

POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
MAX_RECORDS_PER_CYCLE = int(os.environ.get("NYC_API_LIMIT", "1000"))


async def poll_forever() -> None:
    while True:
        try:
            await asyncio.to_thread(run_ingestion_cycle, MAX_RECORDS_PER_CYCLE)
        except Exception as exc:  # noqa: BLE001 -- top-level loop boundary:
            # one bad cycle (network blip, transient Snowflake error) must
            # never kill the whole long-running process; run_ingestion_cycle
            # already logs a "failed" pipeline_runs row for the specific
            # failure, this is the last-resort net for anything it missed.
            print(f"Scheduler: unexpected error in cycle: {exc}")

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(poll_forever())
