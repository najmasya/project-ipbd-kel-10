# Fase 3 — Machine Learning & Analytics

Panduan untuk Najma melanjutkan project.

---

## 1. Prasyarat — Pipeline Fase 1-2 Harus Jalan

Sebelum mulai Fase 3, pastikan infrastruktur sudah jalan:

```bash
# 1. Jalankan semua container
docker compose -f docker/docker-compose.yaml up -d

# 2. Cek semua service hidup
docker ps --format "table {{.Names}}\t{{.Status}}"

# 3. Buat bucket MinIO
docker exec ipbd-spark-master python /opt/spark/src/scripts/init_minio_buckets.py

# 4. Seed data historis (2018-sekarang)
docker exec ipbd-airflow-webserver python /opt/airflow/src/scripts/seed_data.py

# 5. Trigger pipeline batch (manual)
#    Buka http://localhost:8080 → trigger DAG batch_pipeline

# 6. (Opsional) Jalankan streaming MT5
#    Di laptop kamu: python src/streaming/mt5_producer.py
```

### Cek data sudah masuk:
- **MinIO Console**: http://localhost:9001 (`minio_admin` / `minio_pass123`)
  - Bucket `bronze-batch/` harus ada folder: `inflation/`, `exchange_rate/`, `gold_price/`
  - Bucket `bronze-streaming/` akan terisi jika MT5 dan Spark Streaming jalan
- **PostgreSQL**: tabel `pipeline_logs` harus terisi log dari batch pipeline

---

## 2. Yang Harus Kamu Kerjakan (Fase 3)

### A. ML Training — `src/ml/train.py`
Sudah disediakan skeleton. Yang perlu kamu lakukan:
1. Baca dataset dari `s3a://silver-batch/feature_engineering_dataset/`
2. Training model (Linear Regression + Random Forest)
3. Log experiment ke **MLflow** (`http://localhost:5000`)
4. Daftarkan model terbaik ke **Model Registry**

```bash
# Jalankan training (via Airflow atau manual di spark)
docker exec ipbd-airflow-webserver python -m src.ml.train
```

### B. MLflow Experiment Tracking
- UI: http://localhost:5000
- Semua parameter, metrics, dan artifact model tercatat otomatis
- Model terbaik bisa dipromosikan ke **Production** stage dari UI MLflow

### C. Model Inference — `src/ml/inference.py`
Setelah model terdaftar di MLflow Registry:
1. Load model versi Production
2. Ambil data terbaru dari feature engineering
3. Prediksi harga emas
4. Simpan hasil ke PostgreSQL (tabel `gold_price_predictions`)

```bash
docker exec ipbd-airflow-webserver python -m src.ml.inference
```

### D. Airflow DAG — `dags/training_dag.py`
DAG `ml_training_pipeline` sudah siap, jalan setiap **Senin jam 09:00**.
- Task `train_ml_models` → training + MLflow
- Task `run_inference` → prediksi
- **On failure**: otomatis kirim alert ke Telegram grup

---

## 3. Struktur File yang Relevan

```
PROJECT_IPBD/
├── src/ml/
│   ├── __init__.py
│   ├── train.py          ← Training + MLflow (isi logika kamu)
│   ├── inference.py      ← Inference (isi logika kamu)
│   └── utils.py          ← Helper: baca dataset, koneksi PostgreSQL, setup MLflow
│
├── dags/
│   └── training_dag.py   ← DAG training (sudah alert callback)
│
├── config/
│   ├── spark.conf        ← Konfigurasi Spark
│   ├── mlflow.conf       ← Konfigurasi MLflow
│   └── alert_rules.yaml  ← Rule alert business (tambah threshold prediksi)
│
├── scripts/
│   ├── telegram_alert.py ← Fungsi alert (import & gunakan)
│   └── pipeline_logger.py← Logging otomatis
│
└── sql/
    └── gold_layer.sql    ← Schema tabel gold_price_predictions & lainnya
```

---

## 4. Alert System yang Sudah Aktif

### System Alert (otomatis dari Fase 1-2)
| Kejadian | Trigger ke Telegram |
|---|---|
| Airflow DAG gagal | ✅ `on_failure_callback` |
| Service down (PostgreSQL, Kafka, Spark, dll) | ✅ health checker tiap 60 detik |
| CPU/RAM/Disk > 90% | ✅ resource monitor tiap 30 detik |
| Pipeline log gagal | ✅ tercatat di `pipeline_logs` |

### Business Alert (kamu perlu tambah di `scripts/telegram_alert.py`)
| Kejadian | Cara |
|---|---|
| Prediksi harga emas > threshold | Panggil `send_business_alert()` dari inference |
| Anomali XAUUSD | Panggil `send_business_alert()` dari stream processing |

### Cara Kirim Alert dari Kode Kamu
```python
from scripts.telegram_alert import send_business_alert

send_business_alert(
    metric="Gold Price Prediction",
    value=125000.0,
    threshold=120000.0,
    direction="above",
)
```

---

## 5. Verifikasi — APA YANG HARUS KAMU LAKUKAN

- [ ] 1. Infrastruktur jalan (docker compose up)
- [ ] 2. Data batch sudah masuk ke MinIO Bronze (cek di console)
- [ ] 3. Jalankan processing pipeline: `etl_batch_to_silver.py` + `feature_engineering.py`
- [ ] 4. Cek dataset sudah di `s3a://silver-batch/feature_engineering_dataset/`
- [ ] 5. **ISI** `src/ml/train.py` dengan logika training sesungguhnya
- [ ] 6. **ISI** `src/ml/inference.py` dengan logika prediksi
- [ ] 7. Jalankan training → cek MLflow UI
- [ ] 8. Daftarkan model ke Production di MLflow
- [ ] 9. Jalankan inference → cek PostgreSQL `gold_price_predictions`
- [ ] 10. Tambah business alert jika prediksi melebihi threshold

---

## 6. Troubleshooting

| Masalah | Solusi |
|---|---|
| Spark can't connect to MinIO | Cek `spark.hadoop.fs.s3a.endpoint` di `config/spark.conf` |
| Airflow DAG not found | Cek DAG ada di folder `dags/`, restart scheduler |
| MLflow can't save artifact | Cek bucket `mlflow-artifacts` sudah dibuat |
| Telegram alert not sent | Cek `.env` sudah isi `TELEGRAM_BOT_TOKEN` & `TELEGRAM_CHAT_ID` |
| PostgreSQL connection refused | Tunggu container postgres healthcheck selesai |

---

**Good luck!** Kalau ada masalah, cek log container: `docker logs <container_name>`
