"""Concept tests: dbt layering rules and the check catalog (no Snowflake needed)."""

import csv
from pathlib import Path

import yaml

from quality.gate import BLOCKING, build_checks

DBT = Path(__file__).resolve().parents[2] / "dbt"
MODELS = DBT / "models"


def _sql(layer: str) -> dict[str, str]:
    return {p.stem: p.read_text() for p in (MODELS / layer).glob("*.sql")}


def test_only_staging_reads_raw_sources():
    """I learned: staging is the ONLY layer that touches raw tables. If a mart
    read source() directly, a raw column rename would break it silently
    instead of being fixed once in staging."""
    for name, sql in _sql("staging").items():
        assert "source(" in sql and "ref(" not in sql, name
    for layer in ("intermediate", "marts"):
        for name, sql in _sql(layer).items():
            assert "source(" not in sql, f"{layer}/{name} bypasses staging"
            assert "ref(" in sql, name


def test_materializations_match_the_layer_contract():
    """I learned: staging/intermediate are views (cheap, always fresh); marts
    are tables because dashboards read them repeatedly."""
    project = yaml.safe_load((DBT / "dbt_project.yml").read_text())
    cfg = project["models"]["datapulse"]
    assert cfg["staging"]["+materialized"] == "view"
    assert cfg["intermediate"]["+materialized"] == "view"
    assert cfg["marts"]["+materialized"] == "table"


def test_every_model_is_documented_in_yaml():
    documented = set()
    for yml in MODELS.rglob("*.yml"):
        documented |= {m["name"] for m in (yaml.safe_load(yml.read_text()).get("models") or [])}
    all_models = {p.stem for p in MODELS.rglob("*.sql")}
    assert all_models <= documented, all_models - documented


def test_check_catalog_seed_matches_the_real_quality_gate():
    """I learned: the seed tells dbt which checks are blocking. If it drifts
    from quality/gate.py, the health marts would misclassify failures. This
    ties the warehouse's catalog to the code that actually runs."""
    with open(DBT / "seeds" / "quality_check_catalog.csv") as f:
        catalog = {row["check_name"]: row["severity"] for row in csv.DictReader(f)}
    code = {name: sev for name, sev, _ in build_checks((1, None))}
    code["source_schema_matches"] = BLOCKING  # runs outside build_checks
    assert catalog == code
    assert len(catalog) == 12


def test_profiles_contain_no_secrets():
    """Credentials only via env_var(); nothing literal is committed."""
    text = (DBT / "profiles.yml").read_text()
    for key in ("user", "password", "account"):
        line = next(ln for ln in text.splitlines() if ln.strip().startswith(f"{key}:"))
        assert "env_var(" in line, line
