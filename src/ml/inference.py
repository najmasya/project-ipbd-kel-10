import os
import mlflow
import pandas as pd
import numpy as np
from datetime import datetime, date
from src.ml.utils import setup_mlflow, get_spark_session, save_predictions_to_postgres

MLFLOW_MODEL_NAME = "gold_price_predictor"
FEATURES = [
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
]


def load_latest_model():
    client = mlflow.tracking.MlflowClient()
    try:
        latest_versions = client.get_latest_versions(MLFLOW_MODEL_NAME, stages=["Production"])
        if not latest_versions:
            latest_versions = client.get_latest_versions(MLFLOW_MODEL_NAME, stages=["None"])
        if not latest_versions:
            raise ValueError("No model versions found")

        model_version = latest_versions[0]
        model_uri = f"models:/{MLFLOW_MODEL_NAME}/{model_version.version}"
        model = mlflow.sklearn.load_model(model_uri)
        print(f"Loaded model: {MLFLOW_MODEL_NAME} v{model_version.version} (stage: {model_version.current_stage})")
        return model, model_version.version
    except Exception as e:
        print(f"Could not load from registry: {e}")
        local_paths = ["models/random_forest_inflasi.pkl", "models/linear_regression_inflasi.pkl"]
        import joblib
        for p in local_paths:
            if os.path.exists(p):
                artifact = joblib.load(p)
                model = artifact["model"]
                print(f"Loaded local model from {p}")
                return model, "local"
        raise


def prepare_latest_features(spark):
    df = spark.read.parquet("s3a://silver-batch/feature_engineering_dataset")
    last_row = df.orderBy(df.month_key.desc()).limit(1).toPandas()
    if last_row.empty:
        raise ValueError("No feature data found")
    X = last_row[FEATURES].values
    return X, last_row


def run_inference():
    setup_mlflow()
    spark = get_spark_session("ML_Inference")
    model, version = load_latest_model()
    X, metadata = prepare_latest_features(spark)
    spark.stop()

    prediction = model.predict(X)[0]

    today = date.today()
    from dateutil.relativedelta import relativedelta
    target_month = (today.replace(day=1) + relativedelta(months=1))

    pred_df = pd.DataFrame([{
        "prediction_date": today,
        "target_month": target_month,
        "model_name": MLFLOW_MODEL_NAME,
        "model_version": str(version),
        "predicted_value": round(prediction, 2),
        "actual_value": None,
        "upper_bound": round(prediction * 1.1, 2),
        "lower_bound": round(prediction * 0.9, 2),
    }])

    save_predictions_to_postgres(pred_df)
    print(f"Prediction for {target_month}: {prediction:.2f}")


if __name__ == "__main__":
    run_inference()
