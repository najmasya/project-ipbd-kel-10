import streamlit as st
import pandas as pd
from trino.dbapi import connect

st.set_page_config(page_title="Gold Trend", layout="wide")

TRINO_HOST = "trino"
TRINO_PORT = 8082


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="streamlit", catalog="postgresql", schema="public")
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=cols)


st.title(" Tren Harga Emas")

df = query("""
    SELECT date, gold_price_rp, gold_price_usd, usd_idr_rate, inflation_rate
    FROM fact_market_daily
    ORDER BY date DESC
    LIMIT 365
""")

col1, col2, col3 = st.columns(3)
if not df.empty:
    col1.metric("Harga Emas Terakhir (Rp/gram)", f"Rp {df.iloc[0]['gold_price_rp']:,.0f}")
    col2.metric("USD/IDR", f"{df.iloc[0]['usd_idr_rate']:,.0f}")
    col3.metric("Inflasi Terakhir", f"{df.iloc[0]['inflation_rate']:.2f}%")

st.line_chart(df.set_index("date")[["gold_price_rp", "gold_price_usd"]])
st.dataframe(df, use_container_width=True)
