import streamlit as st
import pandas as pd
from trino.dbapi import connect
from auth import require_auth

st.set_page_config(
    page_title="IPBD Gold Price Dashboard",
    page_icon="",
    layout="wide",
)

require_auth()

TRINO_HOST = "trino"
TRINO_PORT = 8082


@st.cache_data(ttl=300)
def query_trino(sql: str) -> pd.DataFrame:
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="streamlit",
        catalog="postgresql",
        schema="public",
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=cols)


st.title(" Gold Price Forecasting Dashboard")
st.markdown("Monitoring harga emas nasional, forecasting, dan pipeline big data")

tab1, tab2, tab3, tab4 = st.tabs([
    " Gold Trend",
    " Forecasting",
    " XAUUSD Live",
    " System Monitor",
])

with tab1:
    st.header("Tren Harga Emas Nasional")
    try:
        df = query_trino("""
            SELECT date, gold_price_rp, gold_price_usd, usd_idr_rate, inflation_rate
            FROM fact_market_daily
            ORDER BY date DESC
            LIMIT 365
        """)
        st.line_chart(df.set_index("date")["gold_price_rp"])
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Gagal memuat data: {e}")

with tab2:
    st.header("Hasil Forecasting")
    try:
        df = query_trino("""
            SELECT target_month, model_name, predicted_value, actual_value
            FROM gold_price_predictions
            ORDER BY target_month DESC
            LIMIT 24
        """)
        st.dataframe(df, use_container_width=True)

        latest = df[df["model_name"] == "random_forest"].head(1)
        if not latest.empty:
            st.metric(
                "Prediksi Bulan Depan",
                f"Rp {latest['predicted_value'].values[0]:,.0f}",
            )
    except Exception as e:
        st.error(f"Gagal memuat forecasting: {e}")

with tab3:
    st.header("Pergerakan XAUUSD Real-Time")
    try:
        df = query_trino("""
            SELECT date, xauusd_open, xauusd_high, xauusd_low, xauusd_close, xauusd_avg_volatility
            FROM fact_market_daily
            WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
            ORDER BY date DESC
        """)
        st.line_chart(df.set_index("date")["xauusd_close"])
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Gagal memuat XAUUSD: {e}")

with tab4:
    st.header("Kesehatan Pipeline")
    try:
        df = query_trino("""
            SELECT metric_date, pipeline_status, alerts_active, last_batch_sync, last_stream_lag
            FROM dashboard_metrics
            ORDER BY metric_date DESC
            LIMIT 7
        """)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Gagal memuat monitoring: {e}")

    try:
        alerts = query_trino("""
            SELECT alert_type, alert_name, severity, message, occurred_at
            FROM xauusd_alerts
            WHERE resolved_at IS NULL
            ORDER BY occurred_at DESC
            LIMIT 10
        """)
        if not alerts.empty:
            st.subheader(" Alert Aktif")
            st.dataframe(alerts, use_container_width=True)
    except Exception as e:
        st.error(f"Gagal memuat alert: {e}")
