# DataPulse Learning Log

## Day 1 — 2026-09-01
**Component built:** Project scaffold, Snowflake connection client, raw-layer schema, NYC Open Data API reader, batch loader, end-to-end single-run script.

**What I learned:**
- Snowflake's default cursor returns tuples (position-based access); passing `DictCursor` returns dicts keyed by column name, which decouples code from `SELECT` column order.
- `cur.executemany()` batches an insert into one request instead of one round-trip per row — the N+1 problem applies to raw DB-API cursors, not just ORMs.
- The NYC Open Data (Socrata) API paginates via `$limit`/`$offset`, not a cursor/token — meaning pagination isn't airtight against concurrent dataset changes.
- The source dataset has no stable `trip_id`, so I generate one deterministically (hash of vendor_id + pickup_datetime + fare_amount) rather than randomly, so re-ingesting the same row twice yields the same ID — a prerequisite for a uniqueness check to mean anything in Day 2.

**Why it matters for interviews:**
- Connection lifecycle management (context manager, warehouse billing while idle) is a concrete cost-and-correctness story, not just "I connected to a database."
- Deterministic vs. random ID generation is a small decision with a real downstream consequence (data quality checks becoming meaningful vs. trivially always-passing) — good example of thinking two steps ahead in a pipeline design.

**Concept tests written:**
- `tests/learning/test_snowflake_concepts.py` — DictCursor vs. tuple cursor, executemany batching, context-manager cleanup guarantee.
- `tests/learning/test_nyc_api_concepts.py` — offset-based pagination, deterministic trip_id hashing, record mapping skip behavior.

**Tomorrow:** Day 2 — Great Expectations, all 6 quality checks, results loaded to `raw.quality_results`.

## AWS Glue / Redshift / PySpark path — 2026-09-09
**Component built:** A parallel transformation path targeting AWS Glue + Redshift + PySpark specifically (added ahead of a TCS Data Engineer interview requiring those three skills, which the Snowflake+dbt path doesn't touch).

**What I learned:**
- Splitting transformation logic (`transforms/quality_transforms_core.py`, zero AWS imports) from cloud I/O (`transforms/glue_run_quality_job.py`, Glue-specific) means the actual PySpark logic is unit-testable locally with a real Spark session — Glue's `GlueContext`/`DynamicFrame` can't be instantiated outside Glue's managed runtime without heavy mocking.
- `Window.partitionBy().orderBy().rowsBetween(-N, -1)` — ending at `-1`, not `0` — excludes the current row from its own rolling baseline. Including it (`0`) lets an extreme value pull its own comparison mean toward itself, making outliers look less anomalous the more extreme they are. Proved this with a real Spark test using a 9999.0 spike.
- Redshift's `executemany()` still issues N individual INSERT statements over the wire — there's no true multi-row batching at the protocol level like some other drivers offer. The idiomatic bulk-load path is `COPY` from an S3-staged file, which lets Redshift's compute slices load their portion of the file in parallel — a consequence of it being a columnar MPP store, not a style preference.
- A shared local Spark session (module-scoped pytest fixture) can only be stopped once — creating and stopping a second `SparkSession` in another test killed the single underlying JVM context for every test after it. Fixed by having every test share the same fixture.

**Why it matters for interviews:**
- Being able to point at a real, locally-run test that proves a specific window-function bug (look-ahead bias) doesn't occur is a stronger answer than describing the concept in words.
- Knowing *when* to reach for dbt vs. PySpark vs. Redshift COPY vs. executemany — and being able to say why — is what separates "I've used these tools" from "I understand the engine constraints that make one the right choice."

**Concept tests written:** `tests/learning/test_pyspark_window_concepts.py` — 4 tests, all run against a real local Spark session (not mocked).

## Continuous deployment (Docker + asyncio scheduler) — 2026-09-12
**Component built:** `ingestion/scheduler.py` (asyncio polling loop pulled forward from the Day 3 plan), `Dockerfile`, `docker-compose.yml` — so ingestion can run continuously on always-on hardware instead of one-shot on a laptop.

**What I learned:**
- `asyncio.to_thread(sync_func, *args)` runs a blocking synchronous function (the Day 1 ingestion cycle, built on sync `requests`/`snowflake-connector` calls) in a worker thread so `await`ing it doesn't freeze the event loop — necessary the moment a second concurrent poller gets added later.
- A cron job re-launching `run_once.py` every 60 seconds pays for a fresh interpreter and Snowflake connection each time; a single long-running asyncio loop avoids that and matches Docker's expectation of one persistent foreground process per container (`restart: unless-stopped` supervises a process, not a sequence of short jobs).
- The `try/except` in `poll_forever` wraps only the ingestion cycle call, not the sleep — proved with a test that injects a `RuntimeError` and confirms the loop still reaches `asyncio.sleep()` afterward rather than crashing the whole container on one bad cycle.
- Testing an "infinite" loop: mock `asyncio.sleep` to raise a sentinel exception on its first call, so the loop runs exactly one real iteration and then exits via that exception — lets you assert on loop *behavior* without an actual infinite loop or real waiting in the test suite.

**Why it matters for interviews:** shows you can reason about what changes (and what doesn't) when moving code from "runs once locally" to "runs forever in production" — connection lifecycle, failure isolation per cycle, and how to make an intentionally-infinite loop testable.

**Concept tests written:** `tests/learning/test_scheduler_concepts.py` — 2 tests, mocking the cycle function and sleep to prove call order and failure isolation without a real 60-second wait.


## Idempotent daily batch + cron — 2026-09-27
**Component built:** Replaced the 60-second polling loop with a cron-triggered, date-parameterized daily batch that overwrites one day's partition in Snowflake raw. Removed `ingestion/scheduler.py`.

**Why the scheduling decision changed:** On 2026-09-12 I argued against cron because the cadence was 60 seconds (a fresh interpreter + connection every minute, and Docker expects a long-running process). The source turned out to be a *historical* daily dataset, so the cadence became once a day. At that cadence the trade-off flips: a supervised always-on process idles 99.9% of the time, while cron + a one-shot `docker compose run --rm` container is simpler and standard. Same reasoning, different input -> different answer.

**What I learned:**
- The old reader always started at offset 0, so every run re-fetched and re-inserted the same first 1,000 rows. "Deterministic IDs" alone don't make reloads idempotent -- the *load* has to use them.
- Content-hash key + MERGE isn't enough: a corrected source row gets a new hash, so MERGE keeps the stale row too. Partition overwrite (delete the logical day, re-insert) inside one transaction is idempotent for identical reloads *and* correct for corrections.
- Hashing only vendor|pickup|fare collides on distinct trips; the id now hashes every source column in a fixed order.
- Socrata offset paging needs `$order=:id` -- without a stable order, pages can skip or repeat rows.
- Logical date vs run date: the batch processes an explicit `--date`, defaulting to the same calendar day in 2023, so any day can be rerun or backfilled with one command.
- `write_pandas` = PUT to an internal stage + COPY INTO: Snowflake's bulk path, the same "stage then COPY" idea as Redshift COPY FROM S3.
- Snowflake DDL auto-commits, so the temp-table CREATE must run before BEGIN.
- No CLUSTER BY: a clustering key enables Automatic Clustering (billed credits) with no benefit at this volume; daily inserts already group micro-partitions by date.

**Concept tests written:** `tests/learning/test_nyc_api_concepts.py` (rewritten, 7 tests), `tests/learning/test_idempotent_load_concepts.py` (7 tests: logical dates, nullable Int64, transaction ordering, rollback on failure, empty-batch guard).

## Quality gate (Great Expectations) + Slack alerts — 2026-09-28
**Component built:** `quality/gate.py` (12 checks in two tiers), `alerts/slack.py`, quarantine table, per-check results in `raw.quality_results`, wired into `ingestion/run_once.py`.

**Design:**
- 5 **blocking** checks (source schema matches, trip_id not null, trip_id unique, pickup not null, row count within 0.5x-2x of the previous successful run). Any failure -> nothing loads, run marked `blocked`, Slack alert, non-zero exit.
- 7 **row-level** checks (dropoff > pickup, fare 0-500, distance 0-100, passengers 1-6, payment type valid, rate code valid, PU/DO location 1-265). Failing rows go to `raw.taxi_trips_quarantine` with the names of the checks they failed; everything else loads. Slack warns when any check fails on more than 5% of rows.
- Good rows and quarantined rows are swapped in the same transaction, so a rerun never leaves them out of sync.

**What I learned:**
- A gate that blocks on every rule never loads real TLC data (refunds are negative fares). Tiering by *what a failure means* -- untrustworthy batch vs bad row -- keeps "gated" true and the pipeline running.
- The schema check must run on the *source* columns, not my own mapped frame (which always matches, since I build it). It would have caught my early wrong-dataset bug.
- Socrata omits null keys from JSON, so the source schema is the union of keys across records, not one record's keys.
- GE column-map expectations ignore nulls: a null passenger_count passes the 1-6 check. Completeness and validity are separate questions.
- GE's `result_format="COMPLETE"` returns `unexpected_index_list`, so the same suite that produces metrics also tells me exactly which rows to quarantine -- no duplicated rules in pandas.
- 12 checks = 13 GE expectations (location range is one check over two columns). Be exact.
- Bandit B608: SQL identifiers can't be bound parameters. The fix isn't just `# nosec` -- it's an allowlist assert so the claim "identifiers never come from input" is enforced.
- Alerting must never raise: a Slack outage can't be allowed to lose the pipeline_runs record.
- First full-day numbers (2023-01-01): 76,752 rows, 76,752 distinct trip_ids, loaded in 26.5s -- no exact duplicates in the source, so uniqueness is a meaningful blocking check.

**Concept tests written:** `tests/learning/test_quality_gate_concepts.py` (11, real GE), `tests/learning/test_alerting_concepts.py` (3, incl. "blocked run never reaches the load step").
