"""Concept test: render the real Streamlit script against a fake API.

Skipped when Streamlit isn't installed (CI installs only the API deps);
run locally with the dashboard requirements to exercise it.
"""

from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = Path(__file__).resolve().parents[2] / "dashboard" / "app.py"

FAKE = {
    "/pipeline/status": {
        "status": "degraded", "reason": "latest run quarantined 6.9% of rows",
        "hours_since_last_success": 2.0, "success_rate_7d": 0.857,
        "latest_run": {"status": "success", "logical_date": "2023-09-28", "rows_loaded": 118583,
                       "quarantine_rate": 0.0694},
    },
    "/pipeline/runs": [
        {"started_at": str(datetime(2026, 9, 28, 21, 0)), "logical_date": str(date(2023, 9, 28)),
         "status": "success", "rows_loaded": 118583, "rows_quarantined": 8846,
         "quarantine_rate": 0.0694, "checks_failed": 5, "blocking_failures": 0,
         "duration_seconds": 64.6, "run_id": "a"},
        {"started_at": str(datetime(2026, 9, 28, 1, 0)), "logical_date": str(date(2023, 1, 3)),
         "status": "blocked", "rows_loaded": 0, "rows_quarantined": 0, "quarantine_rate": None,
         "checks_failed": 1, "blocking_failures": 1, "duration_seconds": 5.0, "run_id": "b"},
    ],
    "/quality/checks": [
        {"run_id": "a", "started_at": "2026-09-28 21:00:00", "check_name": "fare_amount_0_500",
         "severity": "row", "passed": False, "failure_rate": 0.01},
        {"run_id": "a", "started_at": "2026-09-28 21:00:00", "check_name": "trip_id_unique",
         "severity": "blocking", "passed": True, "failure_rate": 0.0},
    ],
    "/metrics/trips/daily": [
        {"pickup_date": "2023-09-28", "trips": 118583, "revenue": 3100000.0},
        {"pickup_date": "2023-09-27", "trips": 111655, "revenue": 2900000.0},
    ],
}


def test_dashboard_renders_status_metrics_and_charts(monkeypatch):
    """I learned: Streamlit's AppTest runs the real script headlessly, so the
    dashboard is tested like any other code -- here against a fake API."""
    import dashboard.api_client as client

    monkeypatch.setattr(client, "get", lambda path, **params: FAKE[path])
    at = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not at.exception
    assert "DEGRADED" in at.warning[0].value
    labels = [m.label for m in at.metric]
    assert "7-day success rate" in labels and "Quarantine rate (latest)" in labels
    assert at.caption[-2].value == "All 5 blocking checks passed on this run."
