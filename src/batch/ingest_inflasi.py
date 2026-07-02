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


def year_to_th(year):
    return year - 1900


def ingest_inflasi(year: int):
    start_ts = time.time()
    th_value = year_to_th(year)
    url = (
        f"https://webapi.bps.go.id/v1/api/list/"
        f"model/data/lang/ind/domain/0000/var/1/th/{th_value}/key/{BPS_API_KEY}"
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
        f"inflation/json/{filename}",
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/json",
    )

    records = []
    datacontent = raw.get("datacontent", {})
    year_padded = f"{th_value:04d}"
    prefix = f"99991{year_padded}"

    for key, value in datacontent.items():
        if key.startswith(prefix):
            month_str = key.replace(prefix, "")
            try:
                month = int(month_str)
            except (ValueError, TypeError):
                continue
            if 1 <= month <= 12:
                val = value
                if isinstance(val, str):
                    val = val.replace(",", ".")
                try:
                    val_float = float(val)
                except (ValueError, TypeError):
                    val_float = None
                records.append({
                    "year": year,
                    "month": month,
                    "inflation_rate": val_float,
                    "ingested_at": datetime.utcnow().isoformat(),
                })

    if records:
        df = pd.DataFrame(records)
        parquet_bytes = BytesIO()
        df.to_parquet(parquet_bytes, index=False)
        parquet_bytes.seek(0)
        client.put_object(
            BUCKET_BRONZE,
            f"inflation/parquet/inflasi_{year}.parquet",
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
