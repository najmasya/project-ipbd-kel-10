#!/bin/bash
# Start streaming pipeline: MT5 -> Kafka -> Spark -> Silver

echo "=== Starting Streaming Pipeline ==="

# Step 1: Check if Spark consumer is already running
if docker exec ipbd-spark-master bash -c "ps aux | grep -v grep | grep spark_consumer.py" 2>/dev/null; then
    echo "[OK] Spark consumer already running"
else
    echo "[...] Submitting Spark consumer in background..."
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
        /opt/spark-apps/streaming/spark_consumer.py \
        > /tmp/spark_streaming.log 2>&1 &"
    echo "[OK] Spark consumer submitted"
fi

echo ""
echo "=== Streaming pipeline ready ==="
echo "MT5 producer: jalankan di terminal terpisah:"
echo "  cd PROJECT_IPBD"
echo '  $env:MT5_SYMBOL="XAUUSDc"'
echo "  python src/streaming/mt5_producer.py"
echo ""
echo "Cek hasil setelah 2 menit:"
echo "  docker exec ipbd-minio ls /data/silver-streaming/xauusd_ohlc/"
