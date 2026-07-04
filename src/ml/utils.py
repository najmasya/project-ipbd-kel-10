import os
import mlflow

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


def _s3_storage_options():
    endpoint = MINIO_ENDPOINT
    if not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"
    return {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "endpoint_url": endpoint,
    }


def setup_mlflow():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
    if experiment is None:
        experiment_id = mlflow.create_experiment(MLFLOW_EXPERIMENT_NAME)
    else:
        experiment_id = experiment.experiment_id
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    return experiment_id


def read_dataset():
    import pandas as pd
    return pd.read_parquet(
        "s3://silver-batch/feature_engineering_dataset",
        storage_options=_s3_storage_options(),
    )


def read_latest_features():
    import pandas as pd
    df = pd.read_parquet(
        "s3://silver-batch/feature_engineering_dataset",
        storage_options=_s3_storage_options(),
    )
    return df.sort_values("bulan_ke").tail(1)


def save_predictions_to_postgres(predictions_df, table_name="gold_price_predictions"):
    import pandas as pd
    from sqlalchemy import create_engine

    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@postgres:5432/pipeline_db"
    )
    predictions_df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Saved {len(predictions_df)} predictions to {table_name}")
