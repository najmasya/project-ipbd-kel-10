import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, avg, lag, month, year, lit, when, coalesce, round, to_date, date_format,
    first, last, max, min, sum, concat,
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

    merged = (
        inflation
        .join(exchange, on="month_key", how="left")
        .join(gold, on="month_key", how="left")
        .orderBy("month_key")
    )

    try:
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
        merged = merged.join(xauusd, on="month_key", how="left")
    except Exception as e:
        print(f"[INFO] XAUUSD streaming not available for features, filling 0. Error: {e}")
        merged = (
            merged
            .withColumn("avg_xauusd_close", lit(0).cast("double"))
            .withColumn("avg_xauusd_spread", lit(0).cast("double"))
            .withColumn("avg_xauusd_volatility", lit(0).cast("double"))
        )

    merged = merged.withColumn("month_num", month(to_date(concat(col("month_key"), lit("-01")))))
    merged = merged.withColumn("year_num", year(to_date(concat(col("month_key"), lit("-01")))))
    merged = merged.withColumn("bulan_ke", col("year_num") * 12 + col("month_num"))

    w = Window.orderBy("month_key")
    merged = (
        merged
        .withColumn("inflasi_lag_1", lag("inflation_rate", 1).over(w))
        .withColumn("inflasi_lag_2", lag("inflation_rate", 2).over(w))
        .withColumn("gold_lag_1", lag("avg_gold_rp", 1).over(w))
        .withColumn("gold_lag_2", lag("avg_gold_rp", 2).over(w))
        .withColumn("target_gold_rp_bulan_depan", lead_("avg_gold_rp", 1))
    )

    merged = merged.filter(col("target_gold_rp_bulan_depan").isNotNull())

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
        "gold_lag_2",
        "bulan_ke",
        "target_gold_rp_bulan_depan",
    ).write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "fact_market_daily_v2") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .save()
    print("Feature dataset written to PostgreSQL Gold Layer")


def lead_(col_name, offset):
    from pyspark.sql.functions import lead as _lead
    w = Window.orderBy("month_key")
    return _lead(col_name, offset).over(w)


def insert_fact_market_daily():
    gold_daily = (
        spark.read.parquet(f"{SILVER_BATCH}/gold_price_clean")
        .select(
            col("date"),
            col("gold_price_rp_per_gram").alias("gold_price_rp"),
            col("gold_price_usd_per_oz").alias("gold_price_usd"),
            col("usd_idr_rate"),
        )
    )

    exchange_daily = (
        spark.read.parquet(f"{SILVER_BATCH}/exchange_rate_clean")
        .select("date", "usd_idr_close")
    )

    inflation_monthly = (
        spark.read.parquet(f"{SILVER_BATCH}/inflation_clean")
        .withColumn("month_key", date_format(col("month_date"), "yyyy-MM"))
        .select("month_key", "inflation_rate")
    )

    daily = gold_daily.join(exchange_daily, on="date", how="left")
    daily = daily.withColumn("month_key", date_format(col("date"), "yyyy-MM"))
    daily = daily.join(inflation_monthly, on="month_key", how="left")

    try:
        xauusd = spark.read.parquet(SILVER_STREAM)
        xauusd_daily = (
            xauusd
            .withColumn("date", to_date(col("window_start")))
            .groupBy("date")
            .agg(
                first("open", ignorenulls=True).alias("xauusd_open"),
                max("high").alias("xauusd_high"),
                min("low").alias("xauusd_low"),
                last("close", ignorenulls=True).alias("xauusd_close"),
                avg("avg_spread").alias("xauusd_avg_spread"),
                avg("avg_volatility").alias("xauusd_avg_volatility"),
                sum("tick_count").alias("xauusd_tick_count"),
            )
        )
        daily = daily.join(xauusd_daily, on="date", how="left")
    except Exception as e:
        print(f"[INFO] XAUUSD streaming not available, skipping. Error: {e}")
        daily = (
            daily
            .withColumn("xauusd_open", lit(0).cast("double"))
            .withColumn("xauusd_high", lit(0).cast("double"))
            .withColumn("xauusd_low", lit(0).cast("double"))
            .withColumn("xauusd_close", lit(0).cast("double"))
            .withColumn("xauusd_avg_spread", lit(0).cast("double"))
            .withColumn("xauusd_avg_volatility", lit(0).cast("double"))
            .withColumn("xauusd_tick_count", lit(0).cast("bigint"))
        )

    final = daily.select(
        "date",
        col("gold_price_rp").cast("decimal(15,2)"),
        col("gold_price_usd").cast("decimal(10,2)"),
        col("usd_idr_rate").cast("decimal(10,2)"),
        col("inflation_rate").cast("decimal(6,3)"),
        col("xauusd_open").cast("decimal(10,5)"),
        col("xauusd_high").cast("decimal(10,5)"),
        col("xauusd_low").cast("decimal(10,5)"),
        col("xauusd_close").cast("decimal(10,5)"),
        col("xauusd_avg_spread").cast("decimal(8,5)"),
        col("xauusd_avg_volatility").cast("decimal(10,5)"),
        col("xauusd_tick_count"),
    )

    final.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "fact_market_daily") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .save()

    print(f"fact_market_daily written: {final.count()} rows")


if __name__ == "__main__":
    build_features()
    insert_fact_market_daily()
