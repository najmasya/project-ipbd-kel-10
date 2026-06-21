import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_XAUUSD", "xauusd_raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_pass123")
BRONZE_PATH = "s3a://bronze-streaming/xauusd_ticks"
CHECKPOINT_PATH = "/tmp/spark-checkpoints/xauusd_stream"

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
    .appName("XAUUSD_Streaming_Consumer")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()
)

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

df_parsed = (
    df_raw
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("processed_at", current_timestamp())
)

query = (
    df_parsed.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("symbol")
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Spark Streaming consumer started. Writing to {BRONZE_PATH}")
query.awaitTermination()
