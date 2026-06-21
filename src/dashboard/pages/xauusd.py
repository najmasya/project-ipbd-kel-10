import streamlit as st
import pandas as pd
from trino.dbapi import connect

st.set_page_config(page_title="XAUUSD Live", layout="wide")

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


st.title(" Pergerakan XAUUSD")

df = query("""
    SELECT date, xauusd_open, xauusd_high, xauusd_low, xauusd_close, xauusd_avg_spread, xauusd_avg_volatility
    FROM fact_market_daily
    WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
    ORDER BY date DESC
""")

col1, col2, col3, col4 = st.columns(4)
if not df.empty:
    last = df.iloc[0]
    col1.metric("Close", f"{last['xauusd_close']:.5f}")
    col2.metric("High", f"{last['xauusd_high']:.5f}")
    col3.metric("Low", f"{last['xauusd_low']:.5f}")
    col4.metric("Spread", f"{last['xauusd_avg_spread']:.5f}")

st.line_chart(df.set_index("date")[["xauusd_close", "xauusd_avg_volatility"]])
st.dataframe(df, use_container_width=True)
