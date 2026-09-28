"""
Load the Glue job's star-schema Parquet from S3 into Redshift Serverless.

    python scripts/aws/load_redshift.py --ddl                     # create schema + tables
    python scripts/aws/load_redshift.py --year 2023 --month 1     # load one month
    python scripts/aws/load_redshift.py --verify                  # row counts + a star join

Why the Redshift Data API (boto3 'redshift-data') instead of a JDBC/psycopg
driver: it's an HTTPS API authenticated with IAM. No database password, no
driver, and no VPC networking from the caller to the warehouse -- this runs
from a laptop or Groot exactly as it would from a Lambda or Step Function.

Why batch_execute_statement: the Data API runs the whole batch as ONE
transaction. For a month that is: stage the Parquet into a temp table, delete
that month from the fact, insert from the stage. A failure anywhere rolls
back all of it -- the same partition-overwrite pattern as the Snowflake path.

Why DELETE and not TRUNCATE for dimensions: in Redshift, TRUNCATE commits
the current transaction immediately, which would break the all-or-nothing
batch. DELETE stays inside the transaction.

Why IAM_ROLE default: COPY reads S3 using the IAM role attached to the
Redshift namespace as its default. No role ARN or keys appear in SQL.
"""

import argparse
import calendar
import os
import re
import sys
import time
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[2]
DDL_FILE = REPO_ROOT / "redshift" / "ddl.sql"
DIMENSIONS = ["dim_date", "dim_time", "dim_zone", "dim_payment_type", "dim_rate_code"]

# S3 bucket naming rules. The bucket name ends up inside SQL text, so it is
# validated before use -- see _copy_sql for why that matters.
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def _bucket() -> str:
    bucket = os.environ["DATAPULSE_BUCKET"]
    if not _BUCKET_RE.fullmatch(bucket):
        raise ValueError(f"Invalid S3 bucket name: {bucket!r}")
    return bucket


def _copy_sql(table: str, s3_prefix: str) -> str:
    """COPY statement. S3 paths can't be bind parameters in COPY, so this is
    built as text; safe because the table comes from a fixed list and the
    bucket is regex-validated above."""
    return f"COPY {table} FROM '{s3_prefix}' IAM_ROLE default FORMAT AS PARQUET"  # nosec B608


def dimension_sqls(bucket: str) -> list[str]:
    sqls = []
    for dim in DIMENSIONS:
        sqls.append(f"DELETE FROM star.{dim}")  # nosec B608
        sqls.append(_copy_sql(f"star.{dim}", f"s3://{bucket}/curated/star/{dim}/"))
    return sqls


def month_bounds(year: int, month: int) -> tuple[int, int]:
    last_day = calendar.monthrange(year, month)[1]
    return year * 10000 + month * 100 + 1, year * 10000 + month * 100 + last_day


def fact_month_sqls(bucket: str, year: int, month: int) -> list[str]:
    first_key, last_key = month_bounds(year, month)
    prefix = f"s3://{bucket}/curated/star/fact_trips/year={year}/month={month}/"
    return [
        "CREATE TEMP TABLE fact_stage (LIKE star.fact_trips)",
        _copy_sql("fact_stage", prefix),
        f"DELETE FROM star.fact_trips WHERE pickup_date_key BETWEEN {first_key} AND {last_key}",  # nosec B608
        "INSERT INTO star.fact_trips SELECT * FROM fact_stage",
    ]


VERIFY_SQL = """
SELECT 'fact_trips' AS tbl, COUNT(*) AS n FROM star.fact_trips
UNION ALL SELECT 'dim_date', COUNT(*) FROM star.dim_date
UNION ALL SELECT 'dim_time', COUNT(*) FROM star.dim_time
UNION ALL SELECT 'dim_zone', COUNT(*) FROM star.dim_zone
UNION ALL SELECT 'dim_payment_type', COUNT(*) FROM star.dim_payment_type
UNION ALL SELECT 'dim_rate_code', COUNT(*) FROM star.dim_rate_code
ORDER BY 1
"""

# A real star join: every dimension used, pickup and dropoff zones role-played.
STAR_JOIN_SQL = """
SELECT d.day_name, t.day_part, pz.borough AS pickup_borough, dz.borough AS dropoff_borough,
       p.payment_type_desc, COUNT(*) AS trips, SUM(f.total_amount) AS revenue
FROM star.fact_trips f
JOIN star.dim_date d          ON f.pickup_date_key = d.date_key
JOIN star.dim_time t          ON f.pickup_time_key = t.time_key
JOIN star.dim_zone pz         ON f.pu_zone_key = pz.zone_key
JOIN star.dim_zone dz         ON f.do_zone_key = dz.zone_key
JOIN star.dim_payment_type p  ON f.payment_type_key = p.payment_type_key
JOIN star.dim_rate_code r     ON f.rate_code_key = r.rate_code_key
WHERE r.rate_code_desc = 'JFK'
GROUP BY 1, 2, 3, 4, 5
ORDER BY trips DESC
LIMIT 10
"""


def ddl_statements() -> list[str]:
    text = "\n".join(
        line for line in DDL_FILE.read_text().splitlines() if not line.strip().startswith("--")
    )
    return [s.strip() for s in text.split(";") if s.strip()]


class RedshiftDataClient:
    def __init__(self) -> None:
        self.client = boto3.client("redshift-data")
        self.workgroup = os.environ.get("REDSHIFT_WORKGROUP", "datapulse")
        self.database = os.environ.get("REDSHIFT_DATABASE", "dev")

    def _wait(self, statement_id: str) -> dict:
        while True:
            desc = self.client.describe_statement(Id=statement_id)
            if desc["Status"] in ("FINISHED", "FAILED", "ABORTED"):
                if desc["Status"] != "FINISHED":
                    raise RuntimeError(f"Redshift statement {desc['Status']}: {desc.get('Error')}")
                return desc
            time.sleep(2)

    def run_transaction(self, sqls: list[str]) -> None:
        resp = self.client.batch_execute_statement(
            WorkgroupName=self.workgroup, Database=self.database, Sqls=sqls
        )
        self._wait(resp["Id"])

    def query(self, sql: str) -> list[list]:
        resp = self.client.execute_statement(
            WorkgroupName=self.workgroup, Database=self.database, Sql=sql
        )
        self._wait(resp["Id"])
        result = self.client.get_statement_result(Id=resp["Id"])
        columns = [c["name"] for c in result["ColumnMetadata"]]
        rows = [[next(iter(cell.values())) for cell in rec] for rec in result["Records"]]
        return [columns, *rows]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load DataPulse star schema into Redshift")
    parser.add_argument("--ddl", action="store_true", help="create schema and tables")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--verify", action="store_true", help="print row counts and a star join")
    args = parser.parse_args(argv)
    rs = RedshiftDataClient()

    if args.ddl:
        rs.run_transaction(ddl_statements())
        print("DDL applied.")
    if args.year and args.month:
        start = time.monotonic()
        rs.run_transaction(
            dimension_sqls(_bucket()) + fact_month_sqls(_bucket(), args.year, args.month)
        )
        print(f"Loaded {args.year}-{args.month:02d} in {time.monotonic() - start:.1f}s")
    if args.verify:
        for sql in (VERIFY_SQL, STAR_JOIN_SQL):
            for row in rs.query(sql):
                print(" | ".join(str(v) for v in row))
            print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
