# Data Governance — Project IPBD

**Gold Price Forecasting Pipeline**
**Kelompok 10 — IPBD 2026**

---

## 1. Data Lineage

### 1.1 Diagram Alur Data

```
SOURCE                      BRONZE (MinIO)            SILVER (MinIO)           GOLD (PostgreSQL)          SERVING
──────────────────────────────────────────────────────────────────────────────────────────────────────────────
[BPS API] ────ingest_inflasi.py──→ inflation/json/ ──etl_batch_to_silver.py──→ inflation_clean/ ──┐
                                    inflation/parquet/                                              │
                                                                                                    ├──feature_engineering.py──→ fact_market_daily ──→ Dashboard
[Yahoo Finance] ──ingest_kurs.py──→ exchange_rate/ ──etl_batch_to_silver.py──→ exchange_rate_clean/ ─┤                                    (Streamlit + Grafana)
                                                                                                    │
[Yahoo Finance] ──ingest_emas.py──→ gold_price/   ──etl_batch_to_silver.py──→ gold_price_clean/   ──┤
                                                                                                    │
[MT5] ──mt5_producer.py──→ Kafka ──spark_consumer.py──→ xauusd_ohlc/ ────────────────────────────────┘
(xauusd_raw)                              (silver-streaming)                                                   
                                                                                          ┌── gold_price_predictions (ML output)
                                                                                          │
                                                                                    feature_engineering.py
                                                                                          │
                                                                                          ├── target: gold_price_prediction
                                                                                          │
                                                                                    MLflow → train.py → inference.py
```

### 1.2 Detail Alir Data per Layer

| Layer | Storage | Deskripsi | Proses |
|-------|---------|-----------|--------|
| **Source** | Eksternal | BPS API, Yahoo Finance, MetaTrader 5 | API calls, streaming |
| **Bronze** | MinIO `bronze-batch/`, Kafka `xauusd_raw` | Data mentah tanpa transformasi | Ingestion |
| **Silver** | MinIO `silver-batch/`, `silver-streaming/` | Data bersih, tervalidasi, terdeduplikasi | ETL, Streaming |
| **Gold** | PostgreSQL `pipeline_db` | Data siap ML + dashboard + monitoring | Feature Engineering, ML |
| **Serving** | Streamlit, Grafana, Telegram | Visualisasi + Alert | Query, Render |

---

## 2. Data Dictionary

### 2.1 Bronze Layer (MinIO)

#### bronze-batch/inflation (BPS API)
| Kolom | Tipe | Deskripsi | Sumber |
|-------|------|-----------|--------|
| year | Integer | Tahun data | BPS API |
| month | Integer | Bulan (1-12) | BPS API |
| inflation_rate | Float/NULL | Tingkat inflasi bulanan (%) | BPS API |
| ingested_at | Timestamp | Waktu ingest | System |

#### bronze-batch/exchange_rate (Yahoo Finance)
| Kolom | Tipe | Deskripsi | Sumber |
|-------|------|-----------|--------|
| Date | Timestamp | Tanggal | Yahoo Finance (`IDR=X`) |
| Open | Float | Kurs buka | Yahoo Finance |
| High | Float | Kurs tertinggi | Yahoo Finance |
| Low | Float | Kurs terendah | Yahoo Finance |
| Close | Float | Kurs tutup | Yahoo Finance |
| Volume | Integer | Volume transaksi | Yahoo Finance |

#### bronze-batch/gold_price (Yahoo Finance)
| Kolom | Tipe | Deskripsi | Sumber |
|-------|------|-----------|--------|
| Date | Timestamp | Tanggal | Yahoo Finance (`GC=F`) |
| gold_price_usd_per_oz | Float | Harga emas USD/oz | Yahoo Finance |
| usd_idr_rate | Float | Kurs USD/IDR | Yahoo Finance |
| gold_price_rp_per_gram | Float | Harga emas Rp/gram | Hasil konversi |

#### Kafka: xauusd_raw (MetaTrader 5)
| Kolom | Tipe | Deskripsi |
|-------|------|-----------|
| symbol | String | Simbol (XAUUSD) |
| price | Float | Harga terkini |
| tick_volume | Integer | Volume tick |
| spread | Integer | Spread dalam points |
| timestamp | Timestamp | Waktu tick |
| source | String | Sumber ("MT5") |

### 2.2 Silver Layer (MinIO)

#### silver-batch/inflation_clean
| Kolom | Tipe | Deskripsi | Data Quality |
|-------|------|-----------|-------------|
| month_date | Date | Tanggal (YYYY-MM-01) | Hasil konversi year+month |
| year | Integer | Tahun | Original |
| month | Integer | Bulan | Original |
| inflation_rate | Float | Inflasi bulanan (%) | NOT NULL (filtered) |
| ingested_at | Timestamp | Waktu ingest | Original |

#### silver-batch/exchange_rate_clean
| Kolom | Tipe | Deskripsi | Data Quality |
|-------|------|-----------|-------------|
| date | Date | Tanggal | Hasil casting |
| usd_idr_open | Float | Kurs buka | Original |
| usd_idr_high | Float | Kurs tertinggi | Original |
| usd_idr_low | Float | Kurs terendah | Original |
| usd_idr_close | Float | Kurs tutup | NOT NULL (filtered) |
| usd_idr_volume | Integer | Volume | Original |

#### silver-batch/gold_price_clean
| Kolom | Tipe | Deskripsi | Data Quality |
|-------|------|-----------|-------------|
| date | Date | Tanggal | Hasil casting |
| gold_price_usd_per_oz | Float | Harga USD/oz | Original |
| usd_idr_rate | Float | Kurs USD/IDR | Original |
| gold_price_rp_per_gram | Float | Harga Rp/gram | NOT NULL (filtered) |

#### silver-streaming/xauusd_ohlc
| Kolom | Tipe | Deskripsi |
|-------|------|-----------|
| window_start | Timestamp | Awal window 1 menit |
| window_end | Timestamp | Akhir window 1 menit |
| open | Float | Harga buka |
| high | Float | Harga tertinggi |
| low | Float | Harga terendah |
| close | Float | Harga tutup |
| avg_spread | Float | Rata-rata spread |
| avg_volatility | Float | Rata-rata volatilitas |
| tick_count | Integer | Jumlah tick |

### 2.3 Gold Layer (PostgreSQL)

#### fact_market_daily
| Kolom | Tipe | Deskripsi | Constraint |
|-------|------|-----------|------------|
| date | DATE | Tanggal | PRIMARY KEY |
| gold_price_rp | NUMERIC(15,2) | Harga emas Rp/gram | Nullable |
| gold_price_usd | NUMERIC(10,2) | Harga emas USD/oz | Nullable |
| usd_idr_rate | NUMERIC(10,2) | Kurs USD/IDR | Nullable |
| inflation_rate | NUMERIC(6,3) | Inflasi bulanan (%) | Nullable |
| xauusd_open | NUMERIC(10,5) | XAUUSD buka | Default 0 |
| xauusd_high | NUMERIC(10,5) | XAUUSD tertinggi | Default 0 |
| xauusd_low | NUMERIC(10,5) | XAUUSD terendah | Default 0 |
| xauusd_close | NUMERIC(10,5) | XAUUSD tutup | Default 0 |
| xauusd_avg_spread | NUMERIC(8,5) | Rata-rata spread | Default 0 |
| xauusd_avg_volatility | NUMERIC(10,5) | Rata-rata volatilitas | Default 0 |
| xauusd_tick_count | BIGINT | Jumlah tick | Default 0 |
| created_at | TIMESTAMPTZ | Waktu insert | DEFAULT NOW() |
| updated_at | TIMESTAMPTZ | Waktu update | DEFAULT NOW() |

#### fact_market_daily_v2 (Feature Engineering Output)
| Kolom | Tipe | Deskripsi |
|-------|------|-----------|
| month_key | VARCHAR(7) | Periode (YYYY-MM) |
| inflation_rate | Float | Inflasi bulanan |
| avg_usd_idr | Float | Rata-rata kurs USD/IDR |
| avg_gold_rp | Float | Rata-rata harga emas Rp |
| avg_xauusd_close | Float | Rata-rata XAUUSD close |
| avg_xauusd_spread | Float | Rata-rata spread |
| avg_xauusd_volatility | Float | Rata-rata volatilitas |
| inflasi_lag_1 | Float | Inflasi bulan lalu |
| inflasi_lag_2 | Float | Inflasi 2 bulan lalu |
| gold_lag_1 | Float | Harga emas bulan lalu |
| gold_lag_2 | Float | Harga emas 2 bulan lalu |
| bulan_ke | Integer | Indeks bulan absolut |
| target_gold_rp_bulan_depan | Float | Target prediksi (harga emas +1 bulan) |

#### gold_price_predictions
| Kolom | Tipe | Deskripsi | Constraint |
|-------|------|-----------|------------|
| id | BIGSERIAL | PK | PRIMARY KEY |
| prediction_date | DATE | Tanggal prediksi | NOT NULL |
| target_month | DATE | Bulan target | NOT NULL |
| model_name | VARCHAR(50) | Nama model | NOT NULL |
| model_version | VARCHAR(20) | Versi/run ID | Nullable |
| predicted_value | NUMERIC(15,2) | Nilai prediksi | Nullable |
| actual_value | NUMERIC(15,2) | Nilai aktual | Nullable |
| upper_bound | NUMERIC(15,2) | Batas atas | Nullable |
| lower_bound | NUMERIC(15,2) | Batas bawah | Nullable |
| created_at | TIMESTAMPTZ | Waktu insert | DEFAULT NOW() |

#### xauusd_alerts
| Kolom | Tipe | Deskripsi | Constraint |
|-------|------|-----------|------------|
| id | BIGSERIAL | PK | PRIMARY KEY |
| alert_type | VARCHAR(20) | Tipe alert | NOT NULL |
| alert_name | VARCHAR(100) | Nama alert | NOT NULL |
| severity | VARCHAR(10) | Level severity | NOT NULL |
| symbol | VARCHAR(10) | Simbol trading | Nullable |
| current_value | NUMERIC(15,5) | Nilai saat ini | Nullable |
| threshold_value | NUMERIC(15,5) | Nilai threshold | Nullable |
| message | TEXT | Pesan | Nullable |
| occurred_at | TIMESTAMPTZ | Waktu kejadian | NOT NULL |
| resolved_at | TIMESTAMPTZ | Waktu resolve | Nullable |

#### dashboard_metrics
| Kolom | Tipe | Deskripsi | Constraint |
|-------|------|-----------|------------|
| metric_date | DATE | Tanggal metrik | PRIMARY KEY |
| total_predictions | INTEGER | Jumlah prediksi | DEFAULT 0 |
| alerts_active | INTEGER | Jumlah alert aktif | DEFAULT 0 |
| pipeline_status | VARCHAR(20) | Status pipeline | Nullable |
| last_batch_sync | TIMESTAMPTZ | Sinkronisasi batch terakhir | Nullable |
| last_stream_lag | INTERVAL | Lag streaming | Nullable |

#### pipeline_logs
| Kolom | Tipe | Deskripsi | Constraint |
|-------|------|-----------|------------|
| id | BIGSERIAL | PK | PRIMARY KEY |
| pipeline_name | VARCHAR(100) | Nama pipeline | NOT NULL |
| task_name | VARCHAR(100) | Nama task | Nullable |
| status | VARCHAR(20) | Status eksekusi | NOT NULL |
| severity | VARCHAR(10) | Level severity | DEFAULT 'INFO' |
| message | TEXT | Pesan log | Nullable |
| records_count | INTEGER | Jumlah record | DEFAULT 0 |
| duration_ms | INTEGER | Durasi (ms) | DEFAULT 0 |
| started_at | TIMESTAMPTZ | Waktu mulai | NOT NULL |
| finished_at | TIMESTAMPTZ | Waktu selesai | Nullable |

#### alert_log
| Kolom | Tipe | Deskripsi | Constraint |
|-------|------|-----------|------------|
| id | BIGSERIAL | PK | PRIMARY KEY |
| alert_type | VARCHAR(20) | Tipe alert | NOT NULL |
| alert_name | VARCHAR(100) | Nama alert | NOT NULL |
| severity | VARCHAR(10) | Level severity | NOT NULL |
| source | VARCHAR(100) | Sumber alert | Nullable |
| message | TEXT | Pesan | Nullable |
| is_resolved | BOOLEAN | Status resolved | DEFAULT FALSE |
| triggered_at | TIMESTAMPTZ | Waktu trigger | NOT NULL |
| resolved_at | TIMESTAMPTZ | Waktu resolve | Nullable |

#### resource_metrics
| Kolom | Tipe | Deskripsi | Constraint |
|-------|------|-----------|------------|
| id | BIGSERIAL | PK | PRIMARY KEY |
| hostname | VARCHAR(100) | Nama host | NOT NULL |
| cpu_percent | NUMERIC(5,2) | CPU usage (%) | Nullable |
| memory_percent | NUMERIC(5,2) | Memory usage (%) | Nullable |
| memory_used_mb | NUMERIC(10,2) | Memory terpakai (MB) | Nullable |
| disk_percent | NUMERIC(5,2) | Disk usage (%) | Nullable |
| disk_used_mb | NUMERIC(10,2) | Disk terpakai (MB) | Nullable |
| network_rx_mb | NUMERIC(10,2) | Network RX (MB) | Nullable |
| network_tx_mb | NUMERIC(10,2) | Network TX (MB) | Nullable |
| recorded_at | TIMESTAMPTZ | Waktu rekam | NOT NULL DEFAULT NOW() |

---

## 3. Data Quality Rules

### 3.1 Ringkasan Rule per Tahap

| Tahap | File | Baris | Rule | Dampak |
|-------|------|-------|------|--------|
| **Ingest Inflasi** | `ingest_inflasi.py:89-94` | Konversi string→float, jika gagal → NULL | Nilai inflasi invalid jadi NULL |
| **ETL Inflation** | `etl_batch_to_silver.py:34` | `filter(col("inflation_rate").isNotNull())` | Buang baris inflasi NULL |
| **ETL Inflation** | `etl_batch_to_silver.py:35` | `dropDuplicates(["year", "month"])` | Buang duplikat bulan yang sama |
| **ETL Exchange** | `etl_batch_to_silver.py:47` | `filter(col("Close").isNotNull())` | Buang baris kurs NULL |
| **ETL Exchange** | `etl_batch_to_silver.py:48` | `dropDuplicates(["date"])` | Buang duplikat tanggal yang sama |
| **ETL Gold** | `etl_batch_to_silver.py:68` | `filter(col("gold_price_rp_per_gram").isNotNull())` | Buang baris harga NULL |
| **ETL Gold** | `etl_batch_to_silver.py:69` | `dropDuplicates(["date"])` | Buang duplikat tanggal yang sama |
| **FE: XAUUSD null** | `feature_engineering.py:90-96` | Try-except: jika streaming unavailable, isi 0 | Graceful degradation |
| **FE: Coalesce** | `feature_engineering.py:100-102` | `coalesce(col, lit(0))` pada XAUUSD batch | Null streaming field diisi 0 |
| **FE: Streaming agg** | `feature_engineering.py:193,196` | `first(..., ignorenulls=True)` dan `last(..., ignorenulls=True)` | Skip null pada aggregasi window |
| **FE: Coalesce daily** | `feature_engineering.py:218-224` | `coalesce(col, lit(0))` pada XAUUSD harian | Null XAUUSD diisi 0 |
| **FE: Target filter** | `feature_engineering.py:119` | `filter(col("target...").isNotNull())` | Buang baris tanpa target (bulan terakhir) |
| **FE: Type casting** | `feature_engineering.py:227-240` | Casting ke decimal dengan presisi sesuai | Konsistensi tipe data |
| **ML Training** | `train.py:86` | `df.dropna(subset=FEATURES+TARGET)` | Buang baris dengan null di feature/target |
| **Dashboard** | `app.py:162` | `df_pred.dropna()` | Buang null sebelum render chart |

### 3.2 Detail Rule per Dataset

#### Inflation (BPS API)
```
Source:      BPS API → JSON → Parquet
Filter:      inflation_rate IS NOT NULL
Duplicates:  dropDuplicates(year, month)
Format:      month_date = DATE(year, month, 01)
Output:      silver-batch/inflation_clean/
```

#### Exchange Rate (Yahoo Finance)
```
Source:      yfinance IDR=X → Parquet
Filter:      Close IS NOT NULL
Duplicates:  dropDuplicates(date)
Transform:   date = to_date(cast(Date/1000000000 as timestamp))
Columns:     Open→usd_idr_open, High→usd_idr_high, Low→usd_idr_low,
             Close→usd_idr_close, Volume→usd_idr_volume
Output:      silver-batch/exchange_rate_clean/
```

#### Gold Price (Yahoo Finance)
```
Source:      yfinance GC=F → Parquet
Filter:      gold_price_rp_per_gram IS NOT NULL
Duplicates:  dropDuplicates(date)
Transform:   date = to_date(cast(Date/1000000000 as timestamp))
Output:      silver-batch/gold_price_clean/
```

#### XAUUSD Streaming (MT5 → Kafka → Spark)
```
Source:      MT5 → Kafka(xauusd_raw) → Spark Structured Streaming
Window:      1 menit (tumbling window)
Null agg:    first(open, ignorenulls=True), last(close, ignorenulls=True)
Fallback:    Jika streaming unavailable → semua kolom XAUUSD diisi 0
Output:      silver-streaming/xauusd_ohlc/ (Parquet)
```

#### Feature Engineering (Silver → Gold)
```
Source:      4 dataset silver
Join:        LEFT JOIN (inflation, exchange, gold, xauusd) on month_key
Null fill:   XAUUSD coalesce(col, 0)
Lag:         inflasi_lag_1, inflasi_lag_2, gold_lag_1, gold_lag_2
Target:      lead(avg_gold_rp, 1) → target_gold_rp_bulan_depan
Filter:      target_gold_rp_bulan_depan IS NOT NULL (buang row terakhir)
Output:      PostgreSQL fact_market_daily_v2
```

---

## 4. Metadata Sumber Data

| Sumber | Endpoint / Library | Data yang Diambil | Frekuensi | Format Output |
|--------|-------------------|-------------------|-----------|---------------|
| **BPS API** | `https://webapi.bps.go.id` | Inflasi bulanan Indonesia | Bulanan (batch) | JSON → Parquet |
| **Yahoo Finance** | `yfinance` (`IDR=X`) | Kurs USD/IDR | Harian (batch) | Parquet |
| **Yahoo Finance** | `yfinance` (`GC=F`) | Harga emas futures, kurs USD/IDR | Harian (batch) | Parquet |
| **MetaTrader 5** | `MetaTrader5` library | XAUUSD OHLC real-time | Real-time (stream) | Kafka → Parquet |

### 4.1 Detail API Key

| Layanan | Key / Credential | Lokasi | Lingkup |
|---------|-----------------|--------|---------|
| BPS API | `4a67171f05ad5ed1156015dbc801c263` | `ingest_inflasi.py:10` | Akses ke seluruh data BPS |
| MT5 Login | Environment variable `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` | `.env` / environment | Akses akun trading ICMarkets-Demo |
| Telegram Bot | `8683547942:AAFoJRDrsFzaI4yghNw0uMghwVAfXvACRA0` | `.env`, `alerting.yaml` | Bot `@ipbd_alert_bot` |

---

## 5. Data Lifecycle

### 5.1 Pipeline Schedule

| Pipeline | Jadwal | Trigger | Durasi Estimasi |
|----------|--------|---------|-----------------|
| **Batch Ingestion** | Setiap hari 07:00 WIB | Airflow DAG `batch_pipeline` | ~2-5 menit |
| **ETL Bronze → Silver** | Setelah ingestion selesai | Airflow DAG `processing_pipeline` | ~3-5 menit |
| **Feature Engineering** | Setelah ETL selesai | Airflow DAG `processing_pipeline` | ~5-10 menit |
| **ML Training** | Mingguan | Airflow DAG `training_dag` | ~10-30 menit |
| **Streaming** | Real-time (saat MT5 aktif) | Manual `mt5_producer.py` | Selama MT5 aktif |
| **Health Checker** | Setiap 60 detik | Container `health-checker` | ~2-5 detik |

### 5.2 Storage Retention

| Layer | Path | Mode Penulisan | Retensi |
|-------|------|---------------|---------|
| Bronze Batch | `s3a://bronze-batch/inflation/` | Append per tahun | Permanen (data historis) |
| Bronze Batch | `s3a://bronze-batch/exchange_rate/` | Append | Permanen |
| Bronze Batch | `s3a://bronze-batch/gold_price/` | Append | Permanen |
| Silver Batch | `s3a://silver-batch/*_clean/` | Overwrite | Overwrite tiap eksekusi |
| Silver Stream | `s3a://silver-streaming/xauusd_ohlc/` | Append (per window) | Permanen |
| Gold (PostgreSQL) | `fact_market_daily` | Overwrite | Overwrite tiap FE |
| Gold (PostgreSQL) | `fact_market_daily_v2` | Overwrite | Overwrite tiap FE |
| Gold (PostgreSQL) | `gold_price_predictions` | Append | Append tiap training/ML |
| Gold (PostgreSQL) | `pipeline_logs` | Append | Append tiap eksekusi |
| Gold (PostgreSQL) | `resource_metrics` | Append | Append tiap 30-60 detik |

### 5.3 File Naming Convention

```
bronze-batch/
  inflation/
    json/   inflasi_{year}.json
    parquet/ inflasi_{year}.parquet
  exchange_rate/       {timestamp}.parquet
  gold_price/          {timestamp}.parquet

silver-batch/
  inflation_clean/     {spark_partition_files}
  exchange_rate_clean/{spark_partition_files}
  gold_price_clean/   {spark_partition_files}

silver-streaming/
  xauusd_ohlc/        {spark_partition_files}
```

---

## 6. Data Security & Access Control

### 6.1 Service Credentials

| Service | Username | Password | Akses |
|---------|----------|----------|-------|
| PostgreSQL | `ipbd_user` | `ipbd_pass` | Full access ke `pipeline_db` |
| MinIO (Console) | `minio_admin` | `minio_pass123` | Full access ke semua bucket |
| Airflow (UI) | `admin` | `admin` | Admin panel |
| Grafana (UI) | `admin` | `admin` | View/edit dashboard |
| Streamlit (UI) | `admin` | `admin123` | View dashboard |

### 6.2 Network Security

- Semua service berada dalam Docker network `ipbd-net` (bridge)
- Service internal (Spark, Kafka, MinIO) tidak terekspos ke luar
- Port yang terekspos ke host:
  - Airflow: `8080`
  - Grafana: `3001`
  - Streamlit: `8501`
  - MinIO Console: `9001`
  - MinIO API: `9000`
  - Kafka: `9092`
  - Spark Master: `8090`
  - MLflow: `5000`
  - Trino: `8082`

---

## 7. Data Quality Monitoring

### 7.1 Grafana Alert Rules

| Rule | Query | Threshold | Evaluasi | Notifikasi |
|------|-------|-----------|----------|------------|
| CPU > 90% | `SELECT cpu_percent FROM resource_metrics ORDER BY recorded_at DESC LIMIT 1` | > 90 | Setiap 1 menit | Telegram (`@ipbd_alert_bot`) |
| Memory > 90% | `SELECT memory_percent FROM resource_metrics ORDER BY recorded_at DESC LIMIT 1` | > 90 | Setiap 1 menit | Telegram |
| Disk > 90% | `SELECT disk_percent FROM resource_metrics ORDER BY recorded_at DESC LIMIT 1` | > 90 | Setiap 1 menit | Telegram |
| Pipeline Failed | `SELECT COUNT(*) FROM pipeline_logs WHERE status='failed' AND started_at > NOW() - 5 minutes` | > 0 | Setiap 1 menit | Telegram |

### 7.2 Pipeline Monitoring (alert_utils.py)

| Callback | Trigger | Output |
|----------|---------|--------|
| `alert_on_failure` | Task Airflow gagal | Telegram: `[SYSTEM] Pipeline {name} FAILED` |
| `alert_on_success` | Task Airflow sukses | Telegram: `[SYSTEM] Pipeline {name} SUCCESS` |

### 7.3 Health Checker (resource_monitor.py)

| Metrik | Interval | Target Tabel |
|--------|----------|-------------|
| CPU usage | 30 detik | `resource_metrics` |
| Memory usage | 30 detik | `resource_metrics` |
| Disk usage | 30 detik | `resource_metrics` |
| Network traffic | 30 detik | `resource_metrics` |
| Service health | 60 detik | `pipeline_logs` |

---

## 8. Referensi & Sumber

| Kategori | Sumber | URL / Dokumentasi |
|----------|--------|-------------------|
| Data | BPS API | `https://webapi.bps.go.id` |
| Data | Yahoo Finance | `https://finance.yahoo.com` |
| Data | MetaTrader 5 | `https://www.metatrader5.com/` |
| Orkestrasi | Apache Airflow 2.9.3 | `https://airflow.apache.org/` |
| Stream | Apache Kafka 7.5.0 | `https://kafka.apache.org/` |
| Processing | Apache Spark 3.5.3 | `https://spark.apache.org/` |
| Storage | MinIO | `https://min.io/` |
| Database | PostgreSQL 15 | `https://www.postgresql.org/` |
| Query | Trino 434 | `https://trino.io/` |
| ML | MLflow 2.10.2 | `https://mlflow.org/` |
| Dashboard | Streamlit 1.28 | `https://streamlit.io/` |
| Monitoring | Grafana 13 | `https://grafana.com/` |
| Alert | Telegram Bot API | `https://core.telegram.org/bots/api` |
| Library | scikit-learn | Pedregosa et al., JMLR 2011 |
| Library | pandas | McKinney, SciPy 2010 |


---

*Kelompok 10 — IPBD 2026*
