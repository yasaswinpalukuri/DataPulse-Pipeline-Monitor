# DataPulse — Cloud-Native Pipeline Monitor with Data Quality

![CI](https://github.com/yasaswinpalukuri/DataPulse-Pipeline-Monitor/actions/workflows/ci.yml/badge.svg)

A production-style data pipeline: NYC Open Data / S3 → Great Expectations →
Snowflake (raw) → dbt (staging/intermediate/marts) → FastAPI → Streamlit,
with Slack alerting and full CI (ruff + bandit + pytest + dbt test).

**Status: Day 1 of 8** — see `LEARNING_LOG.md` for build history.

## Day 1 setup

1. `cp .env.example .env` and fill in your Snowflake account details.
2. `pip install -r requirements.txt`
3. `python -m warehouse.apply_schema` — creates the database/schemas/raw tables.
4. `python -m ingestion.run_once` — runs one ingestion cycle against the live NYC Open Data API and logs the result.

## Two transformation paths

- **Snowflake + dbt** (primary, Days 1-8): SQL-native transformations, warehouse-pushdown aggregation.
- **AWS Glue + PySpark + Redshift** (`transforms/`, `warehouse/redshift_*.py`): the same rolling-anomaly and quality-score logic, implemented in PySpark and targeting Redshift, for roles that specifically require Glue/PySpark/Redshift experience. `transforms/quality_transforms_core.py` has zero AWS dependencies and is unit-tested locally with a real Spark session (`pip install -r requirements-aws.txt` first); `transforms/glue_run_quality_job.py` is the thin AWS Glue wrapper around it and only runs inside Glue's managed environment.

## Project layout

See `dbt/`, `ingestion/`, `quality/`, `warehouse/`, `api/`, `dashboard/`,
`alerts/` — each maps to one stage of the pipeline (see `LEARNING_LOG.md`
Day 1 entry for the reasoning).
