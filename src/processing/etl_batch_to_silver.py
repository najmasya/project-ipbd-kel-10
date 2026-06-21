import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, coalesce, lit

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_pass123")

spark = (
    SparkSession.builder
    .appName("ETL_Batch_To_Silver")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

BRONZE_BASE = "s3a://bronze-batch"
SILVER_BASE = "s3a://silver-batch"


def clean_inflation():
    df = spark.read.parquet(f"{BRONZE_BASE}/inflation/")
    df_clean = (
        df
        .withColumn("month_date", to_date(col("year").cast("string") + "-" + col("month").cast("string") + "-01"))
        .filter(col("inflation_rate").isNotNull())
        .dropDuplicates(["year", "month"])
        .select("month_date", "year", "month", "inflation_rate", "ingested_at")
    )
    df_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/inflation_clean")
    print(f"Inflation silver: {df_clean.count()} rows")


def clean_exchange_rate():
    df = spark.read.parquet(f"{BRONZE_BASE}/exchange_rate/")
    df_clean = (
        df
        .withColumn("date", to_date("Date"))
        .filter(col("Close").isNotNull())
        .dropDuplicates(["date"])
        .select(
            "date",
            col("Open").alias("usd_idr_open"),
            col("High").alias("usd_idr_high"),
            col("Low").alias("usd_idr_low"),
            col("Close").alias("usd_idr_close"),
            col("Volume").alias("usd_idr_volume"),
            "ingested_at",
        )
    )
    df_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/exchange_rate_clean")
    print(f"Exchange rate silver: {df_clean.count()} rows")


def clean_gold_price():
    df = spark.read.parquet(f"{BRONZE_BASE}/gold_price/")
    df_clean = (
        df
        .withColumn("date", to_date("Date"))
        .filter(col("gold_price_rp_per_gram").isNotNull())
        .dropDuplicates(["date"])
        .select(
            "date",
            "gold_price_usd_per_oz",
            "usd_idr_rate",
            "gold_price_rp_per_gram",
            "ingested_at",
        )
    )
    df_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/gold_price_clean")
    print(f"Gold price silver: {df_clean.count()} rows")


if __name__ == "__main__":
    clean_inflation()
    clean_exchange_rate()
    clean_gold_price()
    print("Batch ETL to Silver Layer complete")
