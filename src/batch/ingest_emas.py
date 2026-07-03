import os
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, date
from minio import Minio
from io import BytesIO

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_pass123")
BUCKET_BRONZE = "bronze-batch"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def log_pipeline(status, message, records=0, duration=0, severity="INFO"):
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "ipbd_user"),
            password=os.getenv("POSTGRES_PASSWORD", "ipbd_pass"),
            dbname=os.getenv("POSTGRES_DB", "pipeline_db"),
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_logs
                (pipeline_name, status, severity, message, records_count, duration_ms, started_at, finished_at)
            VALUES ('ingest_emas', %s, %s, %s, %s, %s, NOW(), NOW())
        """, (status, severity, message, records, duration))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[LOG] Failed: {e}")


def ingest_gold_price(start_date: str = None, end_date: str = None):
    start_ts = time.time()
    if not end_date:
        end_date = date.today().isoformat()
    if not start_date:
        start_date = (date.today() - timedelta(days=365)).isoformat()

    ticker = yf.Ticker("GC=F")
    df_gold = ticker.history(start=start_date, end=end_date)

    if df_gold.empty:
        log_pipeline("warning", f"No data {start_date} to {end_date}", severity="WARNING")
        return df_gold

    ticker_kurs = yf.Ticker("IDR=X")
    df_kurs = ticker_kurs.history(start=start_date, end=end_date)
    kurs = df_kurs["Close"] if not df_kurs.empty else pd.Series(index=df_gold.index, data=15800)
    kurs_series = kurs.reindex(df_gold.index, method='ffill')

    df = df_gold.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    df["symbol"] = "XAU/USD"
    df["gold_price_usd_per_oz"] = df["Close"]
    df["usd_idr_rate"] = kurs_series.reindex(df_gold.index, method="ffill").values
    df["gold_price_rp_per_gram"] = (df["gold_price_usd_per_oz"] * df["usd_idr_rate"]) / 31.1035
    df["ingested_at"] = datetime.utcnow().isoformat()

    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)

    parquet_bytes = BytesIO()
    df.to_parquet(parquet_bytes, index=False)
    parquet_bytes.seek(0)

    filename = f"emas_global_{start_date}_{end_date}.parquet"
    client.put_object(
        BUCKET_BRONZE,
        f"gold_price/{filename}",
        parquet_bytes,
        length=parquet_bytes.getbuffer().nbytes,
        content_type="application/parquet",
    )

    duration = int((time.time() - start_ts) * 1000)
    log_pipeline("success", f"Ingested {len(df)} rows", len(df), duration, severity="INFO")
    return df


def get_last_date_in_minio():
    try:
        objs = list(client.list_objects(BUCKET_BRONZE, prefix="gold_price/", recursive=True))
        latest = None
        for obj in objs:
            if not obj.object_name.endswith(".parquet"):
                continue
            resp = client.get_object(BUCKET_BRONZE, obj.object_name)
            df = pd.read_parquet(BytesIO(resp.read()))
            if "Date" in df.columns:
                last = df["Date"].max()
                if latest is None or last > latest:
                    latest = last
        if latest is not None:
            return pd.to_datetime(latest).date()
        return None
    except Exception:
        return None


if __name__ == "__main__":
    try:
        last_date = get_last_date_in_minio()
        if last_date:
            start = (last_date + timedelta(days=1)).isoformat()
            print(f"Last data: {last_date}, fetching from {start}")
        else:
            start = (date.today() - timedelta(days=365)).isoformat()
            print(f"No existing data, fetching from {start}")
        ingest_gold_price(start_date=start)
    except Exception as e:
        log_pipeline("failed", str(e), severity="FATAL")
