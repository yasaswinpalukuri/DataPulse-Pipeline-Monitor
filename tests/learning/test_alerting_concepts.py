"""Concept tests: Slack alerting must never break a run."""

import requests

from alerts import slack


def test_missing_webhook_skips_instead_of_raising(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    assert slack.send_alert("hello") is False


def test_slack_outage_is_swallowed(monkeypatch):
    """I learned: the alert is a side effect. If Slack is down, the run's own
    outcome must still be logged to pipeline_runs -- so never raise here."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.invalid/x")

    def boom(*a, **k):
        raise requests.ConnectionError("slack down")

    monkeypatch.setattr(slack.requests, "post", boom)
    assert slack.send_alert("hello") is False


def test_blocked_run_loads_nothing_and_alerts(monkeypatch):
    """I learned: 'gated' means the load step is never reached on a blocking
    failure -- not 'load it and log a warning'."""
    import ingestion.run_once as run_once

    calls = {"loaded": False, "alerts": [], "runs": []}
    rows = [{"trip_id": "a", "pickup_datetime": "2023-01-01T00:00:00"}] * 2  # duplicate ids
    monkeypatch.setattr(run_once, "fetch_trips_for_date", lambda *a, **k: (rows, 0, set()))
    monkeypatch.setattr(run_once, "previous_run_total", lambda: None)
    monkeypatch.setattr(run_once, "log_quality_results", lambda recs: None)
    monkeypatch.setattr(
        run_once, "overwrite_day_partition", lambda *a: calls.__setitem__("loaded", True)
    )
    monkeypatch.setattr(run_once, "send_alert", lambda text: calls["alerts"].append(text))
    monkeypatch.setattr(run_once, "log_pipeline_run", lambda rec: calls["runs"].append(rec))

    from datetime import date
    record = run_once.run_ingestion_cycle(date(2023, 1, 1))

    assert record["status"] == "blocked"
    assert calls["loaded"] is False
    assert len(calls["alerts"]) == 1 and "BLOCKED" in calls["alerts"][0]
    assert calls["runs"][0]["rows_ingested"] == 0
