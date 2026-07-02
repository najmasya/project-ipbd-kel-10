import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, window, avg, max, min, count, stddev, first, last, lit,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_pass123")

spark = (
    SparkSession.builder
    .appName("ETL_Stream_Reprocess")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

BRONZE_STREAM = "s3a://bronze-streaming/xauusd_ticks"
SILVER_STREAM = "s3a://silver-streaming/xauusd_ohlc"


def reprocess_historical():
    """Batch reprocess historical Bronze data to Silver (backfill)."""
    df = spark.read.parquet(BRONZE_STREAM)

    df_clean = (
        df
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .filter(col("mid").isNotNull())
        .dropDuplicates(["symbol", "timestamp"])
    )

    df_ohlc = (
        df_clean
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

    df_ohlc.write.mode("overwrite").parquet(SILVER_STREAM)
    row_count = df_ohlc.count()
    print(f"Stream silver reprocess: {row_count} OHLC rows written")


if __name__ == "__main__":
    reprocess_historical()
