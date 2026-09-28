"""
DataPulse pipeline-health dashboard (Streamlit).

    Streamlit --HTTP--> FastAPI --SQL--> dbt marts in Snowflake

Run locally:  API_URL=http://localhost:8010 streamlit run dashboard/app.py
"""

import pandas as pd
import requests
import streamlit as st

from dashboard import api_client

st.set_page_config(page_title="DataPulse — Pipeline Health", page_icon="📈", layout="wide")


@st.cache_data(ttl=60)  # matches the API cache; data changes once a day
def load(path: str, **params):
    return api_client.get(path, **params)


st.title("DataPulse — Pipeline Health")
st.caption(
    "Daily NYC Taxi batch → Great Expectations gate → Snowflake → dbt marts → FastAPI → here"
)

try:
    status = load("/pipeline/status")
    runs = pd.DataFrame(load("/pipeline/runs", limit=50))
    checks = pd.DataFrame(load("/quality/checks", runs=10))
    trips = pd.DataFrame(load("/metrics/trips/daily", limit=60))
except requests.RequestException as exc:
    st.error(f"Could not reach the DataPulse API at {api_client.API_URL}: {exc}")
    st.stop()

# ---- Overall status -------------------------------------------------------
banner = {"healthy": st.success, "degraded": st.warning}.get(status["status"], st.error)
banner(f"**Pipeline {status['status'].upper()}** — {status['reason']}")

latest = status.get("latest_run") or {}
hours = status.get("hours_since_last_success")
rate_7d = status.get("success_rate_7d")
q_rate = latest.get("quarantine_rate")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Latest run", (latest.get("status") or "—").upper(), str(latest.get("logical_date") or ""))
c2.metric("Hours since last success", "—" if hours is None else f"{hours:.1f}")
c3.metric("7-day success rate", "—" if rate_7d is None else f"{rate_7d:.0%}")
c4.metric("Rows loaded (latest)", f"{latest.get('rows_loaded') or 0:,}")
c5.metric("Quarantine rate (latest)", "—" if q_rate is None else f"{q_rate:.1%}")

# ---- Runs -----------------------------------------------------------------
st.subheader("Recent runs")
if runs.empty:
    st.info("No runs recorded yet.")
else:
    runs["started_at"] = pd.to_datetime(runs["started_at"])
    st.dataframe(
        runs[["started_at", "logical_date", "status", "rows_loaded", "rows_quarantined",
              "quarantine_rate", "checks_failed", "blocking_failures", "duration_seconds"]],
        hide_index=True,
        width="stretch",
        column_config={
            "started_at": st.column_config.DatetimeColumn("Started (UTC)", format="YYYY-MM-DD HH:mm"),
            "quarantine_rate": st.column_config.NumberColumn("Quarantine rate", format="percent"),
            "duration_seconds": st.column_config.NumberColumn("Duration (s)", format="%.1f"),
        },
    )

    left, right = st.columns(2)
    with left:
        st.markdown("**Rows loaded vs quarantined per run**")
        volume = runs[runs["status"] != "blocked"].sort_values("started_at")
        volume = volume.assign(run=volume["started_at"].dt.strftime("%m-%d %H:%M"))
        st.bar_chart(volume.set_index("run")[["rows_loaded", "rows_quarantined"]], stack=True)

    with right:
        st.markdown("**Quality check failure rate — latest gated run**")
        if checks.empty:
            st.info("No quality results yet.")
        else:
            latest_run = checks.sort_values("started_at")["run_id"].iloc[-1]
            last = checks[(checks["run_id"] == latest_run) & (checks["severity"] == "row")]
            st.bar_chart(last.set_index("check_name")["failure_rate"], horizontal=True)
            failed_blocking = checks[
                (checks["run_id"] == latest_run) & (checks["severity"] == "blocking") & (~checks["passed"])
            ]
            if failed_blocking.empty:
                st.caption("All 5 blocking checks passed on this run.")
            else:
                st.caption("Blocking checks failed: " + ", ".join(failed_blocking["check_name"]))

# ---- Trip metrics ----------------------------------------------------------
st.subheader("Trip metrics by pickup date")
if trips.empty:
    st.info("No trip data yet.")
else:
    trips = trips.sort_values("pickup_date").set_index("pickup_date")
    t1, t2 = st.columns(2)
    t1.markdown("**Trips per day**")
    t1.bar_chart(trips["trips"])
    t2.markdown("**Revenue per day (USD)**")
    t2.bar_chart(trips["revenue"])

st.caption("Data refreshes daily after the 21:00 UTC cron run. Cached for 60 s.")
