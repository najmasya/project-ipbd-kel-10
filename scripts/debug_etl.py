from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_pass123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()

print("=== BRONZE EXCHANGE ===")
df = spark.read.parquet("s3a://bronze-batch/exchange_rate/")
print(f"Raw count: {df.count()}")
print(f"Schema: {df.schema}")

df2 = df.withColumn("date", to_date("Date"))
df3 = df2.filter(col("Close").isNotNull())
print(f"After filter not null: {df3.count()}")

df4 = df3.dropDuplicates(["date"])
print(f"After dedup: {df4.count()}")

df5 = df4.select("date", col("Open").alias("usd_idr_open"))
print(f"Final select: {df5.count()}")
df5.show(5)

spark.stop()
