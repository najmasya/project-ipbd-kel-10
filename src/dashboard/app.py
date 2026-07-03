import streamlit as st
import pandas as pd
import numpy as np
from trino.dbapi import connect
from auth import require_auth

st.set_page_config(
    page_title="Gold Price Intelligence",
    page_icon="📊",
    layout="wide",
)

require_auth()

TRINO_HOST = "trino"
TRINO_PORT = 8080


@st.cache_data(ttl=300)
def query_trino(sql: str) -> pd.DataFrame:
    try:
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
    except Exception:
        return pd.DataFrame()


st.title("Gold Price Intelligence Dashboard")
st.markdown(
    "Analisis pergerakan harga emas & forecasting untuk pengambilan keputusan investasi"
)

tab1, tab2 = st.tabs(["📈 Pasar & Tren", "🔮 Forecasting"])

# ═══════════════════════════════════════════
# TAB 1 — Pasar & Tren
# ═══════════════════════════════════════════
with tab1:
    st.header("Pasar & Tren Harga Emas")

    df_market = query_trino("""
        SELECT date, gold_price_rp, gold_price_usd, usd_idr_rate, inflation_rate,
               xauusd_close, xauusd_avg_volatility
        FROM fact_market_daily
        ORDER BY date DESC
        LIMIT 365
    """)

    if df_market.empty:
        st.info("💡 Belum ada data pasar. Pipeline batch akan mengisi data setelah "
                "ingestion pertama.")
    else:
        df_market = df_market.sort_values("date")

        kol1, kol2, kol3 = st.columns(3)
        latest = df_market.iloc[-1]

        def safe_fmt(val, fmt_str, suffix=""):
            if val is None or (isinstance(val, float) and np.isnan(val)):
                return "N/A"
            return fmt_str.format(val) + suffix

        kol1.metric(
            "💍 Harga Emas (Rp/gram)",
            safe_fmt(latest.get("gold_price_rp"), "Rp {:,.0f}"),
        )
        kol2.metric(
            "💱 USD/IDR",
            safe_fmt(latest.get("usd_idr_rate"), "Rp {:,.0f}"),
        )
        kol3.metric(
            "📊 Inflasi",
            safe_fmt(latest.get("inflation_rate"), "{:.2f}", "%"),
        )

        st.subheader("Harga Emas & Kurs Rupiah — 1 Tahun Terakhir")
        chart_market = (
            df_market[["date", "gold_price_rp", "usd_idr_rate"]]
            .set_index("date")
        )
        st.line_chart(chart_market, height=400, use_container_width=True)

        st.subheader("XAUUSD & Volatilitas — 30 Hari Terakhir")
        chart_xau = (
            df_market.tail(30)[["date", "xauusd_close", "xauusd_avg_volatility"]]
            .set_index("date")
        )
        st.line_chart(chart_xau, height=400, use_container_width=True)

# ═══════════════════════════════════════════
# TAB 2 — Forecasting
# ═══════════════════════════════════════════
with tab2:
    st.header("Forecasting Harga Emas")

    df_pred = query_trino("""
        SELECT target_month, model_name, predicted_value, actual_value,
               upper_bound, lower_bound
        FROM gold_price_predictions
        ORDER BY target_month DESC
        LIMIT 24
    """)

    if df_pred.empty:
        st.info("💡 Belum ada hasil prediksi. Menunggu training model ML pertama.")
    else:
        df_pred = df_pred.sort_values("target_month")

        models = df_pred["model_name"].unique().tolist()
        selected_model = st.selectbox("Pilih Model", models, index=0)

        df_model = df_pred[df_pred["model_name"] == selected_model]

        if not df_model.empty:
            latest_pred = df_model.iloc[-1]
            pred_val = latest_pred["predicted_value"]
            if pd.isna(pred_val):
                st.metric(f"🔮 Prediksi Bulan Depan — {selected_model}", "N/A")
            elif pd.notna(latest_pred["actual_value"]):
                delta_val = pred_val - latest_pred["actual_value"]
                st.metric(
                    f"🔮 Prediksi Bulan Depan — {selected_model}",
                    f"Rp {pred_val:,.0f}",
                    delta=f"vs aktual: {delta_val:+,.0f}",
                )
            else:
                st.metric(
                    f"🔮 Prediksi Bulan Depan — {selected_model}",
                    f"Rp {pred_val:,.0f}",
                )

        st.subheader("Actual vs Prediksi + Confidence Band")
        chart_forecast = (
            df_model[
                ["target_month", "predicted_value", "actual_value",
                 "upper_bound", "lower_bound"]
            ]
            .set_index("target_month")
        )
        st.line_chart(chart_forecast, height=400, use_container_width=True)

        st.subheader("Perbandingan Error Model")
        metrics_list = []
        for model in models:
            df_m = df_pred[df_pred["model_name"] == model].dropna(
                subset=["actual_value"]
            )
            if not df_m.empty:
                mae = np.mean(
                    np.abs(df_m["predicted_value"] - df_m["actual_value"])
                )
                rmse = np.sqrt(
                    np.mean(
                        (df_m["predicted_value"] - df_m["actual_value"]) ** 2
                    )
                )
                metrics_list.append({
                    "Model": model,
                    "MAE": round(mae, 2),
                    "RMSE": round(rmse, 2),
                })

        if metrics_list:
            df_metrics = pd.DataFrame(metrics_list)
            st.bar_chart(
                df_metrics.set_index("Model"),
                height=300,
                use_container_width=True,
            )
            st.dataframe(df_metrics, use_container_width=True)
        else:
            st.info("💡 Belum cukup data actual untuk menghitung metrik error model.")

        st.subheader("Riwayat Prediksi")
        df_table = df_pred.sort_values("target_month", ascending=False)
        st.dataframe(
            df_table,
            use_container_width=True,
            column_config={
                "target_month": "Bulan Target",
                "model_name": "Model",
                "predicted_value": st.column_config.NumberColumn(
                    "Nilai Prediksi", format="%.2f"
                ),
                "actual_value": st.column_config.NumberColumn(
                    "Nilai Aktual", format="%.2f"
                ),
                "upper_bound": st.column_config.NumberColumn(
                    "Upper Bound", format="%.2f"
                ),
                "lower_bound": st.column_config.NumberColumn(
                    "Lower Bound", format="%.2f"
                ),
            },
        )
