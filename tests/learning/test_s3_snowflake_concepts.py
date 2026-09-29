"""Concept tests: S3 -> Snowflake via storage integration + COPY INTO."""

import json
import re
from pathlib import Path

import pytest

import ingestion.load_s3_to_snowflake as s3load

ROOT = Path(__file__).resolve().parents[2]


def test_copy_is_idempotent_by_file_load_metadata():
    """I learned: COPY INTO skips files it already loaded (64-day load
    metadata). Setting FORCE = TRUE would reload them and duplicate rows,
    so the idempotency guarantee depends on FORCE never appearing."""
    assert "FORCE" not in s3load.COPY_SQL.upper()
    assert "ON_ERROR = ABORT_STATEMENT" in s3load.COPY_SQL


def test_copy_maps_every_table_column_in_order():
    """19 source columns + 3 metadata columns must line up with the table."""
    table_cols = re.findall(r"^\s{4}(\w+) [A-Z_]+", s3load.CREATE_TABLE_SQL, re.M)
    assert len(table_cols) == len(s3load.PARQUET_FIELDS) + 3 == 22
    assert table_cols[-3:] == ["source_file", "source_file_row", "loaded_at"]


def test_airport_fee_reads_both_casings():
    """I learned: Parquet field names are case-sensitive in Snowflake, and TLC
    files spell it 'airport_fee' in some years and 'Airport_fee' in others."""
    assert "COALESCE($1:airport_fee, $1:Airport_fee)" in s3load.COPY_SQL


def test_stage_uses_integration_not_keys(monkeypatch):
    """I learned: a storage integration means no AWS keys anywhere in
    Snowflake or in code -- Snowflake assumes a narrowly scoped IAM role."""
    monkeypatch.setenv("DATAPULSE_BUCKET", "datapulse-123-us-east-1")
    sql = s3load.create_stage_sql(s3load.bucket())
    assert "STORAGE_INTEGRATION = datapulse_s3" in sql
    assert "AWS_KEY_ID" not in sql and "CREDENTIALS" not in sql
    assert "s3://datapulse-123-us-east-1/raw/tlc/" in sql


def test_bucket_name_is_validated_before_reaching_sql(monkeypatch):
    monkeypatch.setenv("DATAPULSE_BUCKET", "x'; DROP TABLE raw.taxi_trips; --")
    with pytest.raises(ValueError):
        s3load.bucket()


@pytest.mark.parametrize(
    "rows,expected",
    [
        ([("s3://b/raw/tlc/yellow/f.parquet", "LOADED", 3066766, 3066766)], (1, 3066766)),
        ([("Copy executed with 0 files processed.",)], (0, 0)),  # rerun: no-op
    ],
)
def test_copy_result_summary(rows, expected):
    assert s3load.summarize(rows) == expected


def test_snowflake_trust_requires_external_id():
    """I learned: the external ID condition blocks the 'confused deputy' --
    another Snowflake customer can't use their integration to assume my role."""
    trust = json.loads((ROOT / "infra/iam/snowflake-trust.json").read_text())
    stmt = trust["Statement"][0]
    assert stmt["Condition"]["StringEquals"]["sts:ExternalId"] == "__EXTERNAL_ID__"
    policy = json.loads((ROOT / "infra/iam/snowflake-policy.json").read_text())
    actions = {a for s in policy["Statement"] for a in ([s["Action"]] if isinstance(s["Action"], str) else s["Action"])}
    assert actions == {"s3:ListBucket", "s3:GetObject", "s3:GetObjectVersion"}  # read-only
