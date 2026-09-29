# Evidence

Each claim this project makes, the code that implements it, and the real
result that proves it. Commit hashes refer to this repository's history.

| # | Claim | Where it lives | Proof from real runs | Verify it yourself |
|---|---|---|---|---|
| 1 | **Daily batch pipeline** ingesting NYC Taxi trips (**S3 / NYC Open Data API**) into a **Snowflake raw layer**, with **deterministic key hashing** so **reloads are idempotent** | `ingestion/run_once.py`, `ingestion/nyc_api_reader.py`, `warehouse/loader.py`, `ingestion/load_s3_to_snowflake.py`, `deploy/crontab`, `scripts/run_daily.sh` — commits `2bbb647`, `46c1b26` | API path: same day loaded twice → 5,000 rows (not 10,000); full day 76,752 rows = 76,752 distinct `trip_id`s. S3 path: **3,066,766** rows from the S3 raw zone via storage integration + `COPY INTO` — the same count Glue read from that file; rerun loaded **0 files** (per-file load metadata). Scheduled cron run 2026-09-28 21:00 UTC: 118,583 rows, exit 0 | `tests/learning/test_idempotent_load_concepts.py`, `test_s3_snowflake_concepts.py`; `SELECT COUNT(*), COUNT(DISTINCT trip_id) FROM raw.taxi_trips` |
| 2 | **Parallel AWS Glue + PySpark** path loading a **curated star schema (1 fact, 5 dimension tables)** into **Redshift** | `transforms/star_schema_core.py`, `transforms/glue_star_schema_job.py`, `redshift/ddl.sql`, `scripts/aws/load_redshift.py` — commits `1c98e9c`, `3d6973b` | 3,066,766 rows in → 3,066,690 in `fact_trips`; dims 366 / 1,440 / 266 / 7 / 8 rows; Glue 185 s (370 DPU-s ≈ $0.05); Redshift load 35 s; reload still 3,066,690 | `test_star_schema_concepts.py` (real local Spark; asserts exactly 1 fact + 5 dims in the DDL); `python -m scripts.aws.load_redshift --verify` |
| 3 | **Great Expectations suites (12 checks)** gating loads, **Slack alerts** on failures | `quality/gate.py` (suites `raw_taxi_trips` + `source_contract`), `alerts/slack.py`, `dbt/seeds/quality_check_catalog.csv` — commit `457bb90` | 12 checks = 5 blocking + 7 row-level (13 GE expectations; location range spans 2 columns). Truncated 300-row batch **blocked, 0 rows loaded**. 2023-01-02: 62,814 loaded, 2,963 quarantined by named check | `test_quality_gate_concepts.py` (real GE; asserts exactly 12 checks); `raw.pipeline_runs` shows the `blocked` run |
| 4 | **dbt staging → marts**; pipeline-health metrics served through **FastAPI** and a **Streamlit dashboard** | `dbt/` (4 sources, 4 staging, 1 intermediate, 4 marts, 1 seed, 21 tests), `api/`, `dashboard/` — commits `3503ede`, `2a75dd0`, `48f1c71` | `dbt build`: PASS=31, 0 errors, 11 s; freshness PASS. 7 API endpoints, smoke test all 200. Dashboard: `docs/img/dashboard.png` | `test_dbt_layering_concepts.py`, `test_api_concepts.py`, `test_dashboard_concepts.py`; `bash scripts/smoke_api.sh` |
| 5 | **Containerized** and **scheduled with Docker and cron**; **CI on GitHub Actions (pytest, ruff, bandit)** | `Dockerfile`, `Dockerfile.dbt`, `api/Dockerfile`, `dashboard/Dockerfile`, `docker-compose.yml`, `deploy/crontab`, `.github/workflows/ci.yml` | 4 images; cron-triggered runs logged in `logs/daily_run.log`; CI green with ruff, bandit, 72 pytest, `dbt parse`, Streamlit AppTest | GitHub Actions tab |

## Supporting skills demonstrated

| Skill | Evidence |
|---|---|
| S3 | Private, encrypted raw + curated zones with Hive-style `year=/month=` partitions (`scripts/aws/01_s3_and_data.sh`) |
| IAM (least privilege) | Four scoped roles/policies in `infra/iam/`: Glue, Redshift, Snowflake storage integration (external ID); Glue ran first time with its scoped policy |
| Redshift | Serverless, 4-RPU base, usage limit; `DISTSTYLE ALL` dims, `SORTKEY(pickup_date_key)`; Data API + `COPY FORMAT AS PARQUET` |
| Snowflake | `write_pandas` staging, transactional partition overwrite, storage integration + external stage + `COPY INTO` |
| Great Expectations | GE 1.x suites, `result_format=COMPLETE` row indices drive quarantine |
| dbt | Layered models, seed, generic tests, source freshness, schema-name macro |
| FastAPI / Streamlit | Typed responses, liveness vs readiness, dependency overrides in tests; AppTest-rendered dashboard |
| Docker / cron / CI | One-shot batch containers + long-running services; cron wrapper with exit codes; multi-venv CI |
