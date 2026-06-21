-- ============================================
-- Trino Views — Federated Query Layer
-- Menyederhanakan query dashboard
-- ============================================

-- View untuk dashboard harga emas
CREATE OR REPLACE VIEW gold.v_gold_trend AS
SELECT
    date,
    gold_price_rp,
    gold_price_usd,
    usd_idr_rate,
    inflation_rate
FROM postgresql.gold_layer.fact_market_daily
ORDER BY date DESC;

-- View untuk dashboard forecasting
CREATE OR REPLACE VIEW gold.v_forecast AS
SELECT
    p.target_month,
    p.model_name,
    p.predicted_value,
    p.actual_value,
    p.upper_bound,
    p.lower_bound
FROM postgresql.gold_layer.gold_price_predictions p
WHERE p.prediction_date = (
    SELECT MAX(prediction_date) FROM postgresql.gold_layer.gold_price_predictions
);

-- View untuk XAUUSD real-time summary
CREATE OR REPLACE VIEW gold.v_xauusd_summary AS
SELECT
    date,
    xauusd_open,
    xauusd_high,
    xauusd_low,
    xauusd_close,
    xauusd_avg_spread,
    xauusd_avg_volatility
FROM postgresql.gold_layer.fact_market_daily
WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY date DESC;

-- View untuk monitoring pipeline
CREATE OR REPLACE VIEW gold.v_pipeline_health AS
SELECT
    metric_date,
    pipeline_status,
    alerts_active,
    last_batch_sync,
    last_stream_lag
FROM postgresql.gold_layer.dashboard_metrics
ORDER BY metric_date DESC
LIMIT 7;

-- View untuk alert
CREATE OR REPLACE VIEW gold.v_active_alerts AS
SELECT
    alert_type,
    alert_name,
    severity,
    symbol,
    current_value,
    threshold_value,
    message,
    occurred_at
FROM postgresql.gold_layer.xauusd_alerts
WHERE resolved_at IS NULL
ORDER BY occurred_at DESC;
