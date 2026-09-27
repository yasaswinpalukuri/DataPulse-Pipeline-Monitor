# DataPulse — Cloud-Native Pipeline Monitor with Data Quality

![CI](https://github.com/yasaswinpalukuri/DataPulse-Pipeline-Monitor/actions/workflows/ci.yml/badge.svg)

A production-style data pipeline: NYC Open Data / S3 → Great Expectations →
Snowflake (raw) → dbt (staging/intermediate/marts) → FastAPI → Streamlit,
with Slack alerting and full CI (ruff + bandit + pytest + dbt test).

**Status: in progress** — see `LEARNING_LOG.md` for build history.

## Running the daily batch

Each run loads one *logical date* (the same calendar day, replayed from the
2023 dataset) and overwrites that day's partition in Snowflake raw, so reruns
are idempotent.

```
cp .env.example .env                 # fill in Snowflake credentials
docker compose build ingestion
docker compose run --rm ingestion                     # today's calendar day in 2023
docker compose run --rm ingestion --date 2023-01-05   # rerun / backfill a specific day
```

Scheduled with cron on a home server: see `deploy/crontab` and `scripts/run_daily.sh`
(output is appended to `logs/daily_run.log`).

## Setup

1. `cp .env.example .env` and fill in your Snowflake account details.
2. `pip install -r requirements.txt`
3. `python -m warehouse.apply_schema` — creates the database/schemas/raw tables.
4. `python -m ingestion.run_once --date 2023-01-05` — loads one day from the live NYC Open Data API and logs the run.

## Two transformation paths

- **Snowflake + dbt** (primary, Days 1-8): SQL-native transformations, warehouse-pushdown aggregation.
- **AWS Glue + PySpark + Redshift** (`transforms/`, `warehouse/redshift_*.py`): the same rolling-anomaly and quality-score logic, implemented in PySpark and targeting Redshift, for roles that specifically require Glue/PySpark/Redshift experience. `transforms/quality_transforms_core.py` has zero AWS dependencies and is unit-tested locally with a real Spark session (`pip install -r requirements-aws.txt` first); `transforms/glue_run_quality_job.py` is the thin AWS Glue wrapper around it and only runs inside Glue's managed environment.

## Project layout

See `dbt/`, `ingestion/`, `quality/`, `warehouse/`, `api/`, `dashboard/`,
`alerts/` — each maps to one stage of the pipeline (see `LEARNING_LOG.md`
Day 1 entry for the reasoning).
