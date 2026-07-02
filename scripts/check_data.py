from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_pass123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()

for path, name in [
    ("s3a://bronze-batch/exchange_rate/", "Bronze exchange"),
    ("s3a://bronze-batch/gold_price/", "Bronze gold"),
    ("s3a://bronze-batch/inflation/parquet/", "Bronze inflation"),
]:
    try:
        df = spark.read.parquet(path)
        print(f"{name}: {df.count()} rows, columns: {df.columns}")
        df.show(2, truncate=False)
    except Exception as e:
        print(f"{name}: ERROR - {e}")

spark.stop()
