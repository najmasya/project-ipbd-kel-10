import streamlit as st
import pandas as pd
from trino.dbapi import connect

st.set_page_config(page_title="Forecast", layout="wide")

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


st.title(" Hasil Forecasting")

df = query("""
    SELECT target_month, model_name, predicted_value, actual_value, upper_bound, lower_bound
    FROM gold_price_predictions
    ORDER BY target_month DESC
    LIMIT 24
""")

model_choice = st.selectbox("Pilih Model", df["model_name"].unique() if not df.empty else [])
filtered = df[df["model_name"] == model_choice] if not df.empty else df

st.dataframe(filtered, use_container_width=True)

if not filtered.empty:
    st.subheader("Prediksi Terakhir")
    last = filtered.iloc[0]
    st.metric(
        f"Prediksi {last['target_month']}",
        f"Rp {last['predicted_value']:,.0f}",
    )
