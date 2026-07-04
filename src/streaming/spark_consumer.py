import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp, window,
    first, last, max, min, avg, count, stddev, lit,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_XAUUSD", "xauusd_raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_pass123")
SILVER_PATH = "s3a://silver-streaming/xauusd_ohlc"
CHECKPOINT_PATH = "/tmp/spark-checkpoints/xauusd_silver"

schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("mid", DoubleType()),
    StructField("spread", DoubleType()),
    StructField("last_price", DoubleType()),
    StructField("price_change", DoubleType()),
    StructField("volatility", DoubleType()),
    StructField("volume", LongType()),
    StructField("source", StringType()),
])

spark = (
    SparkSession.builder
    .appName("XAUUSD_Streaming_to_Silver")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

df_parsed = (
    df_raw
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .filter(col("mid").isNotNull())
    .dropDuplicates(["symbol", "timestamp"])
    .withColumn("processed_at", current_timestamp())
)

df_ohlc = (
    df_parsed
    .withWatermark("event_time", "1 minute")
    .groupBy(
        col("symbol"),
        window(col("event_time"), "1 minute"),
    )
    .agg(
        first(col("mid")).alias("open"),
        max(col("mid")).alias("high"),
        min(col("mid")).alias("low"),
        last(col("mid")).alias("close"),
        avg(col("spread")).alias("avg_spread"),
        avg(col("volatility")).alias("avg_volatility"),
        count(lit(1)).alias("tick_count"),
        stddev(col("mid")).alias("price_stddev"),
    )
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "open", "high", "low", "close",
        "avg_spread", "avg_volatility",
        "tick_count", "price_stddev",
    )
)

BATCH_LOG_INTERVAL = 10
_batch_count = [0]
_start_time = [None]


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
                (pipeline_name, task_name, status, severity, message, records_count, duration_ms, started_at, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """, ("stream_ingest", "spark_consumer", status, severity, message, records, duration))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[LOG] Failed: {e}")


def write_batch(df, epoch_id):
    global _batch_count, _start_time
    if _start_time[0] is None:
        _start_time[0] = time.time()
    _batch_count[0] += 1

    df.write.mode("append").parquet(SILVER_PATH)
    count = df.count()
    log_pipeline("success", f"Batch {epoch_id}: {count} ticks processed", count, severity="INFO")

    if _batch_count[0] % BATCH_LOG_INTERVAL == 0:
        elapsed = int((time.time() - _start_time[0]) * 1000)
        print(f"[STREAM] {_batch_count[0]} batches processed, {elapsed}ms elapsed")


query = (
    df_ohlc.writeStream
    .outputMode("append")
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="1 minute")
    .start()
)

log_pipeline("success", "Spark Streaming consumer started", severity="INFO")
print(f"Spark Streaming consumer started. Writing clean OHLC to {SILVER_PATH}")
query.awaitTermination()
