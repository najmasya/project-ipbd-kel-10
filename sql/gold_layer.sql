-- Gold Layer — PostgreSQL
-- Data siap pakai untuk dashboard, ML, monitoring & alerting

-- Tabel fakta harian (batch + stream terintegrasi)
CREATE TABLE IF NOT EXISTS fact_market_daily (
    date            DATE PRIMARY KEY,
    gold_price_rp   NUMERIC(15,2),
    gold_price_usd  NUMERIC(10,2),
    usd_idr_rate    NUMERIC(10,2),
    inflation_rate  NUMERIC(6,3),
    xauusd_open     NUMERIC(10,5),
    xauusd_high     NUMERIC(10,5),
    xauusd_low      NUMERIC(10,5),
    xauusd_close    NUMERIC(10,5),
    xauusd_avg_spread   NUMERIC(8,5),
    xauusd_avg_volatility NUMERIC(10,5),
    xauusd_tick_count    BIGINT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Tabel hasil prediksi harga emas
CREATE TABLE IF NOT EXISTS gold_price_predictions (
    id              BIGSERIAL PRIMARY KEY,
    prediction_date DATE NOT NULL,
    target_month    DATE NOT NULL,
    model_name      VARCHAR(50) NOT NULL,
    model_version   VARCHAR(20),
    predicted_value NUMERIC(15,2),
    actual_value    NUMERIC(15,2),
    upper_bound     NUMERIC(15,2),
    lower_bound     NUMERIC(15,2),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Tabel alert / anomali
CREATE TABLE IF NOT EXISTS xauusd_alerts (
    id              BIGSERIAL PRIMARY KEY,
    alert_type      VARCHAR(20) NOT NULL,
    alert_name      VARCHAR(100) NOT NULL,
    severity        VARCHAR(10) NOT NULL,
    symbol          VARCHAR(10),
    current_value   NUMERIC(15,5),
    threshold_value NUMERIC(15,5),
    message         TEXT,
    occurred_at     TIMESTAMPTZ NOT NULL,
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Tabel metrik dashboard
CREATE TABLE IF NOT EXISTS dashboard_metrics (
    metric_date     DATE PRIMARY KEY,
    total_predictions   INTEGER DEFAULT 0,
    alerts_active       INTEGER DEFAULT 0,
    pipeline_status     VARCHAR(20),
    last_batch_sync     TIMESTAMPTZ,
    last_stream_lag     INTERVAL,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Tabel log pipeline (setiap eksekusi tercatat)
CREATE TABLE IF NOT EXISTS pipeline_logs (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    task_name       VARCHAR(100),
    status          VARCHAR(20) NOT NULL,
    severity        VARCHAR(10) DEFAULT 'INFO',
    message         TEXT,
    records_count   INTEGER DEFAULT 0,
    duration_ms     INTEGER DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Tabel riwayat alert
CREATE TABLE IF NOT EXISTS alert_log (
    id              BIGSERIAL PRIMARY KEY,
    alert_type      VARCHAR(20) NOT NULL,
    alert_name      VARCHAR(100) NOT NULL,
    severity        VARCHAR(10) NOT NULL,
    source          VARCHAR(100),
    message         TEXT,
    is_resolved     BOOLEAN DEFAULT FALSE,
    triggered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

-- Tabel resource metrics (CPU, RAM, Disk)
CREATE TABLE IF NOT EXISTS resource_metrics (
    id              BIGSERIAL PRIMARY KEY,
    hostname        VARCHAR(100) NOT NULL,
    cpu_percent     NUMERIC(5,2),
    memory_percent  NUMERIC(5,2),
    memory_used_mb  NUMERIC(10,2),
    disk_percent    NUMERIC(5,2),
    disk_used_mb    NUMERIC(10,2),
    network_rx_mb   NUMERIC(10,2),
    network_tx_mb   NUMERIC(10,2),
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pastikan kolom severity ada (untuk DB yang sudah existing)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'pipeline_logs' AND column_name = 'severity'
    ) THEN
        ALTER TABLE pipeline_logs ADD COLUMN severity VARCHAR(10) DEFAULT 'INFO';
    END IF;
END $$;

-- Index
CREATE INDEX IF NOT EXISTS idx_predictions_target_month ON gold_price_predictions (target_month DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_occurred ON xauusd_alerts (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_type ON xauusd_alerts (alert_type, severity);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_status ON pipeline_logs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_severity ON pipeline_logs (severity, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_log_time ON alert_log (triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_resource_time ON resource_metrics (recorded_at DESC);
