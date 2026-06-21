import streamlit as st
import pandas as pd
from trino.dbapi import connect

st.set_page_config(page_title="System Monitor", layout="wide")

TRINO_HOST = "trino"
TRINO_PORT = 8082


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="streamlit", catalog="postgresql", schema="public")
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=cols)


st.title(" System Monitoring")

health = query("""
    SELECT metric_date, pipeline_status, alerts_active, last_batch_sync, last_stream_lag
    FROM dashboard_metrics
    ORDER BY metric_date DESC
    LIMIT 7
""")
st.subheader("Pipeline Health")
st.dataframe(health, use_container_width=True)

alerts = query("""
    SELECT alert_type, alert_name, severity, symbol, current_value, threshold_value, message, occurred_at
    FROM xauusd_alerts
    WHERE resolved_at IS NULL
    ORDER BY occurred_at DESC
    LIMIT 20
""")
st.subheader("Active Alerts")

if not alerts.empty:
    for _, alert in alerts.iterrows():
        emoji = "🔴" if alert["severity"] == "critical" else "🟡" if alert["severity"] == "warning" else "🔵"
        st.warning(f"{emoji} **{alert['alert_name']}** — {alert['message']}", icon="⚠️")
else:
    st.success("Tidak ada alert aktif")
