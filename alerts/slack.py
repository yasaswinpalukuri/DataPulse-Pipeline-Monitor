"""
Slack alerts via an Incoming Webhook.

Why a webhook and not the Slack Web API/SDK: posting to one channel needs
nothing but an HTTPS POST to a secret URL -- no bot token, no OAuth scopes,
no extra dependency. The URL itself is the credential, so it lives only in
.env (SLACK_WEBHOOK_URL), never in code or git.

Why alerting never raises: an alert is a side effect of a run, not part of
it. If Slack is down or the URL is unset, the run's own outcome (loaded,
blocked, failed) must still be recorded in pipeline_runs -- so failures here
are printed and swallowed.
"""

import os

import requests

TIMEOUT_SECONDS = 10


def send_alert(text: str) -> bool:
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        print(f"[alert skipped: SLACK_WEBHOOK_URL not set] {text}")
        return False
    try:
        response = requests.post(url, json={"text": text}, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        return True
    except requests.RequestException as exc:
        print(f"[alert failed: {exc}] {text}")
        return False
