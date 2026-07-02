# Fase 3 — Machine Learning & Analytics

Panduan untuk Najma melanjutkan project setelah Fase 1 & 2 selesai.

---

## 1. Yang Sudah Dikerjakan di Fase 1 & 2 (oleh Vio)

### Batch Pipeline ✅
```
Airflow DAG (jadwal 07:00) 
  ├─ ingest_inflasi.py  → BPS API → MinIO bronze-batch/inflation/parquet/
  ├─ ingest_kurs.py     → Yahoo Finance → MinIO bronze-batch/exchange_rate/
  └─ ingest_emas.py     → Yahoo Finance → MinIO bronze-batch/gold_price/
        │
        ▼
Spark ETL (etl_batch_to_silver.py) → MinIO silver-batch/
  ├─ inflation_clean/       (102 baris, bulanan 2018-2026)
  ├─ exchange_rate_clean/   (259 baris, harian)
  └─ gold_price_clean/      (252 baris, harian)
```

### Streaming Pipeline ✅
```
MT5 Producer (laptop) → Kafka topic xauusd_raw → Spark Structured Streaming
        │
        ▼
MinIO silver-streaming/xauusd_ohlc/ (OHLC per menit saat MT5 aktif)
```

### Infrastruktur ✅
| Service | URL | Fungsi |
|---|---|---|
| Airflow | http://localhost:8080 (admin/admin) | Orkestrasi batch |
| MinIO Console | http://localhost:9001 (minio_admin/minio_pass123) | Bronze + Silver storage |
| PostgreSQL | localhost:5432 (ipbd_user/ipbd_pass) | Gold Layer + Logging |
| Kafka | localhost:9092 | Message broker streaming |
| Spark Master | http://localhost:8090 | Cluster processing |
| MLflow | http://localhost:5000 | Experiment tracking |
| Trino | http://localhost:8082 | Federated query engine |
| Streamlit | http://localhost:8501 (admin/admin123) | Dashboard |
| Grafana | http://localhost:3001 (admin/admin) | Monitoring + Alerting |

### Alert System ✅
| Alert | Trigger | Channel |
|---|---|---|
| Pipeline failure | Airflow DAG gagal | Telegram @ipbd_alert_bot |
| Service down | PostgreSQL/MinIO/Kafka/Spark mati | Telegram |
| Resource > 90% | CPU/RAM/Disk | Telegram |
| Semua log | Pipeline logs | PostgreSQL `pipeline_logs` |

---

## 2. Cara Start Semua Infrastruktur

```powershell
cd PROJECT_IPBD/docker
docker compose up -d
```

Tunggu ~2 menit, cek:
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Semua container harus `Up`.

### Sekali Jalan (Reset Database)
Kalau ada error migrasi Airflow:
```powershell
docker exec ipbd-postgres psql -U ipbd_user -d pipeline_db -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"
docker compose up -d airflow-init
```

---

## 3. Cara Jalankan Pipeline Batch (Sudah Otomatis)

Trigger sekali, lalu jalan otomatis tiap jam 07:00:
```powershell
docker exec ipbd-airflow-webserver airflow dags trigger batch_pipeline
docker exec ipbd-airflow-webserver airflow dags trigger processing_pipeline
```

Atau trigger manual untuk ETL Bronze → Silver:
```powershell
docker exec ipbd-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minio_admin \
  --conf spark.hadoop.fs.s3a.secret.key=minio_pass123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS \
  --conf spark.sql.legacy.parquet.nanosAsLong=true \
  /opt/spark-apps/processing/etl_batch_to_silver.py
```

---

## 4. Cara Jalankan Streaming (2 Terminal)

### Terminal 1 — Spark Consumer (cukup sekali)
```powershell
docker exec ipbd-spark-master bash -c "nohup /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minio_admin \
  --conf spark.hadoop.fs.s3a.secret.key=minio_pass123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS \
  --conf spark.sql.legacy.parquet.nanosAsLong=true \
  /opt/spark-apps/streaming/spark_consumer.py > /tmp/spark_streaming.log 2>&1 &"
```

### Terminal 2 — MT5 Producer (setiap mau streaming)
Di laptop kamu (bukan di container):
```powershell
cd PROJECT_IPBD
$env:MT5_SYMBOL="XAUUSDc"
$env:MT5_LOGIN="isi_login_mt5"
$env:MT5_PASSWORD="isi_password_mt5"
$env:MT5_SERVER="isi_server_mt5"
python src/streaming/mt5_producer.py
```

Cek hasil setelah ~2 menit:
```powershell
docker exec ipbd-minio ls /data/silver-streaming/xauusd_ohlc/
```
Kalau ada file `.parquet`, streaming berhasil.

---

## 5. Yang Harus Kamu Kerjakan (Fase 3)

### A. Feature Engineering — Gabung Silver → Dataset ML
**File:** `src/processing/feature_engineering.py`

Data saat ini masih terpisah di 3+ tabel Silver. Kamu perlu:
1. Baca `inflation_clean`, `exchange_rate_clean`, `gold_price_clean` dari MinIO Silver
2. Baca `xauusd_ohlc` dari MinIO Silver-streaming (kalau ada)
3. Agregasi harian → bulanan
4. Gabung semua tabel berdasarkan `month_date`
5. Buat fitur baru:
   - Lag features (`inflasi_lag_1`, `inflasi_lag_2`, `gold_lag_1`)
   - Rolling statistics (moving average 3/6 bulan)
   - Scaling / normalisasi
   - Target variable (`target_inflasi_bulan_depan`)
6. Simpan hasil ke **Gold Layer (PostgreSQL)** tabel `fact_market_daily`

**Trigger:**
```powershell
docker exec ipbd-spark-master /opt/spark/bin/spark-submit \
  [spark config] \
  /opt/spark-apps/processing/feature_engineering.py
```

### B. Training ML
**File:** `src/ml/train.py`
- Baca dataset dari Gold Layer (PostgreSQL)
- Train: Linear Regression + Random Forest (boleh tambah XGBoost/LSTM)
- Split: time series split (80/20)
- Log ke **MLflow** (http://localhost:5000)
- Daftarkan model terbaik ke Model Registry (stage: Production)

### C. Inference
**File:** `src/ml/inference.py`
- Load model versi Production dari MLflow
- Baca data terbaru dari PostgreSQL
- Prediksi harga emas bulan depan
- Simpan hasil ke tabel `gold_price_predictions`

### D. Business Alert
Di `scripts/telegram_alert.py`, panggil `send_business_alert()` untuk:
- Prediksi harga emas melebihi threshold tertentu
- Anomali XAUUSD (volatilitas tinggi)

---

## 6. Struktur File yang Relevan

```
PROJECT_IPBD/
├── src/
│   ├── batch/                    # [VIO] Batch ingestion (inflasi, kurs, emas)
│   ├── streaming/                # [VIO] MT5 producer + Spark consumer
│   ├── processing/
│   │   ├── etl_batch_to_silver.py    # [VIO] Bronze → Silver batch
│   │   ├── etl_stream_to_silver.py   # [VIO] Bronze → Silver stream (reprocess)
│   │   └── feature_engineering.py    # [NAJMA] Silver → Gold → Dataset ML
│   ├── ml/
│   │   ├── train.py              # [NAJMA] Training + MLflow
│   │   ├── inference.py          # [NAJMA] Prediksi harga emas
│   │   └── utils.py              # [NAJMA] Helper functions
│   └── dashboard/                # [VIO] Streamlit dashboard
├── dags/
│   ├── batch_pipeline_dag.py     # [VIO] DAG batch ingestion
│   ├── processing_dag.py         # [VIO] DAG ETL + feature engineering
│   └── training_dag.py           # [NAJMA] DAG training ML (mingguan)
├── sql/
│   └── gold_layer.sql            # [VIO] Schema PostgreSQL Gold Layer
├── scripts/
│   ├── telegram_alert.py         # [VIO] Alert Telegram (system + business)
│   ├── pipeline_logger.py        # [VIO] Logger ke PostgreSQL
│   ├── resource_monitor.py       # [VIO] CPU/RAM/Disk monitoring
│   └── seed_data.py              # [VIO] Seed data historis
├── config/
│   ├── spark.conf                # Konfigurasi Spark
│   └── alert_rules.yaml          # Rule alert
├── .env.example                  # Template env vars (isi sendiri!)
└── FASE3_README.md               # ← INI FILE INI
```

---

## 7. Troubleshooting

| Masalah | Solusi |
|---|---|
| Spark can't connect to MinIO | Cek endpoint `http://minio:9000` di spark conf |
| Airflow DAG not found | Cek PYTHONPATH di compose file, restart scheduler |
| MLflow can't save artifact | Cek bucket `mlflow-artifacts` di MinIO |
| Telegram alert not sent | Cek `.env` sudah isi `TELEGRAM_BOT_TOKEN` & `TELEGRAM_CHAT_ID` |
| PostgreSQL connection refused | Tunggu container postgres healthcheck selesai |
| Data streaming kosong | Jalanin MT5 producer dulu |
| Docker API error | Restart Docker Desktop dari tray icon |

---

## 8. Kontak

Kalau ada masalah dengan pipeline Fase 1-2, tanya **Vio**.
Kalau ada pertanyaan tentang ML / feature engineering, diskusi aja.

**Good luck!**
