# AWS runbook — Glue + Redshift star schema path

Region: `us-east-1`. Every command runs from the repo root on the laptop
(AWS CLI v2 configured with the `yasaswin-admin` IAM user, never root).

## Costs (checked 2026-09-28)
| Resource | Pricing | Expected |
|---|---|---|
| S3 | ~$0.023/GB-month | ~50 MB raw + curated: < $0.01 |
| Glue 5.0 job | ~$0.44/DPU-hour, 1-min minimum; 2 × G.1X | ~$0.05–0.10 per monthly run |
| Redshift Serverless | ~$0.375/RPU-hour, per second, 60 s minimum; 4-RPU base | ~$1.50 per *active* hour; $0 idle |

Guards: $10 AWS Budget alert, Glue job timeout 30 min, Redshift usage limit (RPU-hours/day).

## Order
```bash
source scripts/aws/env.sh                 # prints account, region, bucket
bash scripts/aws/01_s3_and_data.sh 2023 01
bash scripts/aws/02_iam.sh
bash scripts/aws/03_glue_job.sh
bash scripts/aws/run_glue.sh 2023 1       # ~2-4 min; prints DPU-seconds
# Redshift Serverless: console steps below (free trial is claimed in the console)
python -m scripts.aws.load_redshift --ddl
python -m scripts.aws.load_redshift --year 2023 --month 1
python -m scripts.aws.load_redshift --verify
```

## Redshift Serverless (console)
1. Redshift console → Redshift Serverless → **Create workgroup** (use custom
   settings; the quick-start default base capacity is far larger than needed).
2. Workgroup `datapulse`, **Base capacity: 4 RPUs**, default VPC/subnets/security group.
3. New namespace `datapulse`, database `dev`. Admin password: generate and save it
   in a password manager (the pipeline itself uses IAM via the Data API, not this password).
4. Associated IAM roles → associate `datapulse-redshift-role` → **Set as default**.
5. After creation: workgroup → **Limits** → Max RPU-hours, e.g. 10 per day,
   action "Turn off user queries". This is a hard spending cap, not just an alert.

## Teardown
```bash
bash scripts/aws/teardown.sh   # Glue job, Redshift workgroup+namespace, IAM roles, bucket, Glue logs
```

## S3 -> Snowflake raw (storage integration + COPY INTO)
```bash
bash scripts/aws/04_snowflake_role.sh            # role + read-only policy; prints the Snowflake SQL
# run the printed CREATE STORAGE INTEGRATION + DESC INTEGRATION in a Snowflake worksheet
bash scripts/aws/04_snowflake_role.sh <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>
# on Groot (DATAPULSE_BUCKET set in .env):
docker compose run --rm --entrypoint python ingestion -m ingestion.load_s3_to_snowflake
```
Rerunning the load is a no-op: COPY INTO skips files it has already loaded.
