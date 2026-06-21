import os
import mlflow
from pyspark.sql import SparkSession

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = "gold_price_forecasting"
MLFLOW_MODEL_NAME = "gold_price_predictor"

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "jdbc:postgresql://postgres:5432/pipeline_db",
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "ipbd_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "ipbd_pass")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_pass123")


def get_spark_session(app_name: str = "ML_App") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def setup_mlflow():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
    if experiment is None:
        experiment_id = mlflow.create_experiment(MLFLOW_EXPERIMENT_NAME)
    else:
        experiment_id = experiment.experiment_id
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    return experiment_id


def read_dataset(spark: SparkSession):
    df = spark.read.parquet("s3a://silver-batch/feature_engineering_dataset")
    return df.toPandas()


def save_predictions_to_postgres(predictions_df, table_name="gold_price_predictions"):
    import pandas as pd
    from sqlalchemy import create_engine

    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@postgres:5432/pipeline_db"
    )
    predictions_df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Saved {len(predictions_df)} predictions to {table_name}")
