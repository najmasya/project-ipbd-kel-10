# Arsitektur Project IPBD — Gold Price Forecasting

```mermaid
graph TB
    subgraph Sumber["📥 SUMBER DATA"]
        A[("BPS API<br/>Inflasi Bulanan")]
        B[("Yahoo Finance<br/>Kurs USD/IDR")]
        C[("Yahoo Finance<br/>Harga Emas")]
        MT5[("MetaTrader 5<br/>XAUUSD")]
    end

    subgraph Fase1["FASE 1 — INGESTION"]
        AF[("Apache Airflow<br/>(batch_pipeline)")]
        K[("Apache Kafka<br/>(topic: xauusd_raw)")]
    end

    subgraph Fase2["FASE 2 — PROCESSING"]
        SP_ETL[("Spark ETL<br/>(Bronze→Silver)")]
        SP_STRM[("Spark Structured<br/>Streaming")]
    end

    subgraph Storage["🗄️ STORAGE LAYER"]
        BB[("Bronze Batch<br/>MinIO")]
        SS[("Silver Stream<br/>MinIO")]
        SB[("Silver Batch<br/>MinIO")]
        PG[("Gold Layer<br/>PostgreSQL")]
    end

    subgraph Fase3["FASE 3 — MACHINE LEARNING"]
        FE[("Feature<br/>Engineering")]
        MLFLOW[("MLflow<br/>Experiment Tracking")]
        MODEL[("Model<br/>Registry")]
        INFER[("Inference")]
    end

    subgraph Fase4["FASE 4 — SERVING & MONITORING"]
        TRINO[("Trino<br/>Federated Query")]
        DASH[("Streamlit<br/>Dashboard")]
        GRAFANA[("Grafana<br/>Monitoring")]
        TG_BUSINESS("📱 Telegram<br/>@ipbd_alert_bot<br/>[BUSINESS] Alert")
        TG_SYSTEM("📱 Telegram<br/>@ipbd_alert_bot<br/>[SYSTEM] Alert")
    end

    A -->|Ingest Inflasi| AF
    B -->|Ingest Kurs| AF
    C -->|Ingest Emas| AF
    AF -->|Data Mentah| BB

    MT5 -->|Streaming| K
    K -->|Consume| SP_STRM
    SP_STRM -->|OHLC/menit| SS

    BB -->|Baca| SP_ETL
    SP_ETL -->|Data Bersih| SB

    SS --> FE
    SB --> FE
    FE -->|Dataset ML| PG

    PG -->|Baca Dataset| MLFLOW
    MLFLOW --> MODEL
    MODEL --> INFER
    INFER -->|Hasil Prediksi| PG

    PG -->|Query| TRINO
    TRINO -->|SQL| DASH

    PG -.->|System Metrics| GRAFANA
    GRAFANA -.->|Notifikasi| TG_SYSTEM

    INFER -.->|Business Rule| TG_BUSINESS

    style Sumber fill:#e1f5fe
    style Fase1 fill:#fff3e0
    style Fase2 fill:#f3e5f5
    style Storage fill:#e8f5e9
    style Fase3 fill:#fff8e1
    style Fase4 fill:#ffebee
```

## Teknologi yang Digunakan

| Layer | Teknologi | Versi |
|---|---|---|
| Orkestrasi Batch | Apache Airflow | 2.9.3 |
| Message Broker | Apache Kafka | 7.5.0 (cp-kafka) |
| Stream Processing | Apache Spark Structured Streaming | 3.5.3 |
| Batch Processing | Apache Spark | 3.5.3 |
| Object Storage | MinIO | latest |
| Relational Database | PostgreSQL | 15 |
| Federated Query | Trino | 434 |
| Experiment Tracking | MLflow | 2.10.2 |
| Dashboard | Streamlit | 1.28 |
| Monitoring | Grafana | latest |
| Alert | Telegram Bot | @ipbd_alert_bot |
| Programming | Python | 3.11 |

## Alur Data per Layer

| Layer | Storage | Deskripsi |
|---|---|---|
| **Bronze Batch** | MinIO `bronze-batch/` | Data mentah dari API BPS, Yahoo Finance |
| **Silver Batch** | MinIO `silver-batch/` | Data bersih: inflation_clean, exchange_rate_clean, gold_price_clean |
| **Silver Stream** | MinIO `silver-streaming/` | OHLC XAUUSD per menit |
| **Gold Layer** | PostgreSQL | Dataset integrasi + hasil prediksi |

## Author

**Vio & Najma** — Kelompok 10, IPBD 2026
