import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, avg, lag, month, year, lit, when, coalesce, round, to_date, date_format,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_pass123")

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "jdbc:postgresql://postgres:5432/pipeline_db",
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "ipbd_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "ipbd_pass")

spark = (
    SparkSession.builder
    .appName("Feature_Engineering")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

SILVER_BATCH = "s3a://silver-batch"
SILVER_STREAM = "s3a://silver-streaming/xauusd_ohlc"


def build_features():
    inflation = (
        spark.read.parquet(f"{SILVER_BATCH}/inflation_clean")
        .withColumn("month_key", date_format(col("month_date"), "yyyy-MM"))
        .select("month_key", "inflation_rate")
    )

    exchange = (
        spark.read.parquet(f"{SILVER_BATCH}/exchange_rate_clean")
        .withColumn("month_key", date_format(col("date"), "yyyy-MM"))
        .groupBy("month_key")
        .agg(
            avg("usd_idr_close").alias("avg_usd_idr"),
        )
        .select("month_key", "avg_usd_idr")
    )

    gold = (
        spark.read.parquet(f"{SILVER_BATCH}/gold_price_clean")
        .withColumn("month_key", date_format(col("date"), "yyyy-MM"))
        .groupBy("month_key")
        .agg(
            avg("gold_price_rp_per_gram").alias("avg_gold_rp"),
        )
        .select("month_key", "avg_gold_rp")
    )

    xauusd = (
        spark.read.parquet(SILVER_STREAM)
        .withColumn("month_key", date_format(col("window_start"), "yyyy-MM"))
        .groupBy("month_key")
        .agg(
            avg("close").alias("avg_xauusd_close"),
            avg("avg_spread").alias("avg_xauusd_spread"),
            avg("avg_volatility").alias("avg_xauusd_volatility"),
        )
        .select(
            "month_key",
            "avg_xauusd_close",
            "avg_xauusd_spread",
            "avg_xauusd_volatility",
        )
    )

    merged = (
        inflation
        .join(exchange, on="month_key", how="left")
        .join(gold, on="month_key", how="left")
        .join(xauusd, on="month_key", how="left")
        .orderBy("month_key")
    )

    merged = merged.withColumn("month_num", month(col("month_key") + "-01"))
    merged = merged.withColumn("year_num", year(col("month_key") + "-01"))
    merged = merged.withColumn("bulan_ke", col("year_num") * 12 + col("month_num"))

    w = Window.orderBy("month_key")
    merged = (
        merged
        .withColumn("inflasi_lag_1", lag("inflation_rate", 1).over(w))
        .withColumn("inflasi_lag_2", lag("inflation_rate", 2).over(w))
        .withColumn("gold_lag_1", lag("avg_gold_rp", 1).over(w))
        .withColumn("target_inflasi_bulan_depan", lead_("inflation_rate", 1))
    )

    merged = merged.filter(col("target_inflasi_bulan_depan").isNotNull())

    merged.write.mode("overwrite").parquet(f"{SILVER_BATCH}/feature_engineering_dataset")
    print(f"Feature engineering complete: {merged.count()} rows")

    merged.write.mode("overwrite").parquet("s3a://gold-layer/dataset_ml")
    print("Dataset saved to gold-layer/dataset_ml")

    merged.select(
        "month_key",
        "inflation_rate",
        "avg_usd_idr",
        "avg_gold_rp",
        "avg_xauusd_close",
        "avg_xauusd_spread",
        "avg_xauusd_volatility",
        "inflasi_lag_1",
        "inflasi_lag_2",
        "gold_lag_1",
        "bulan_ke",
        "target_inflasi_bulan_depan",
    ).write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "gold_layer.fact_market_daily_v2") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .save()
    print("Feature dataset written to PostgreSQL Gold Layer")


def lead_(col_name, offset):
    from pyspark.sql.functions import lead as _lead
    w = Window.orderBy("month_key")
    return _lead(col_name, offset).over(w)


if __name__ == "__main__":
    build_features()
