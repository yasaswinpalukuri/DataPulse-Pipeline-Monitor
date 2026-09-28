"""Apply schema.sql to Snowflake. Run manually: python -m warehouse.apply_schema

Statements are split on ';' and executed individually because the
Snowflake connector does not support multi-statement execution by
default without an extra session parameter — splitting keeps this
explicit and debuggable (if statement 4 fails, you know it's statement
4, not "something in the file").
"""

from pathlib import Path

from dotenv import load_dotenv

from warehouse.snowflake_client import execute

SCHEMA_FILE = Path(__file__).parent / "schema.sql"


def apply_schema() -> None:
    sql_text = SCHEMA_FILE.read_text()
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    for i, statement in enumerate(statements, start=1):
        print(f"[{i}/{len(statements)}] Executing: {statement.splitlines()[0][:60]}...")
        execute(statement)

    print(f"Schema applied: {len(statements)} statements executed.")


if __name__ == "__main__":
    load_dotenv()  # entry point loads config; library modules only read os.environ
    apply_schema()
