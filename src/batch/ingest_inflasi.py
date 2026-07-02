import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, date
from minio import Minio
from io import BytesIO

BPS_API_KEY = os.getenv("BPS_API_KEY", "4a67171f05ad5ed1156015dbc801c263")
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
            VALUES ('ingest_inflasi', %s, %s, %s, %s, %s, NOW(), NOW())
        """, (status, severity, message, records, duration))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[LOG] Failed: {e}")


def ingest_inflasi(year: int):
    start_ts = time.time()
    url = (
        f"https://webapi.bps.go.id/v1/api/list/"
        f"model/ipt/domain/1200/var/206/th/{year}/key/{BPS_API_KEY}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    raw = resp.json()

    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)

    filename = f"bps_inflasi_{year}.json"
    data_bytes = json.dumps(raw, indent=2).encode("utf-8")
    client.put_object(
        BUCKET_BRONZE,
        f"inflation/{filename}",
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/json",
    )

    records = []
    data_content = raw.get("data", [])
    if isinstance(data_content, dict):
        records_data = data_content.get("Data", [])
    elif isinstance(data_content, list) and len(data_content) > 0:
        records_data = data_content[0].get("Data", [])
    else:
        records_data = []

    for item in records_data:
        label = item.get("label", "")
        value_str = item.get("nilai", "").replace(",", ".")
        try:
            value = float(value_str)
        except (ValueError, TypeError):
            value = None
        month = int(label.split()[-1]) if label.split()[-1].isdigit() else None
        if month:
            records.append({
                "year": year,
                "month": month,
                "inflation_rate": value,
                "ingested_at": datetime.utcnow().isoformat(),
            })

    if records:
        df = pd.DataFrame(records)
        parquet_bytes = BytesIO()
        df.to_parquet(parquet_bytes, index=False)
        parquet_bytes.seek(0)
        client.put_object(
            BUCKET_BRONZE,
            f"inflation/inflasi_{year}.parquet",
            parquet_bytes,
            length=parquet_bytes.getbuffer().nbytes,
            content_type="application/parquet",
        )

    duration = int((time.time() - start_ts) * 1000)
    log_pipeline("success", f"Ingested {len(records)} months for {year}", len(records), duration, severity="INFO")
    return records


if __name__ == "__main__":
    current_year = date.today().year
    for y in range(current_year - 8, current_year + 1):
        try:
            ingest_inflasi(y)
        except Exception as e:
            log_pipeline("failed", str(e), severity="FATAL")
