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

