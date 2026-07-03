import os
import joblib
import mlflow
import pandas as pd
import numpy as np
from datetime import datetime, date
try:
    from src.ml.utils import save_predictions_to_postgres, read_latest_features
except ImportError:
    from ml.utils import save_predictions_to_postgres, read_latest_features

MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
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
    "gold_lag_2",
    "bulan_ke",
]


def load_latest_model():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    try:
        client = mlflow.tracking.MlflowClient()
        versions = client.get_latest_versions(MLFLOW_MODEL_NAME, stages=["None", "Production"])
        if versions:
            v = versions[-1]
            model_uri = f"models:/{MLFLOW_MODEL_NAME}/{v.version}"
            model = mlflow.sklearn.load_model(model_uri)
            print(f"Loaded from MLflow: {MLFLOW_MODEL_NAME} v{v.version}")
            return model, str(v.version)
    except Exception as e:
        print(f"MLflow registry failed: {e}")

    for fname in ["linear_regression.pkl", "random_forest.pkl"]:
        fpath = os.path.join(MODELS_DIR, fname)
        if os.path.exists(fpath):
            model = joblib.load(fpath)
            print(f"Loaded from local: {fpath}")
            return model, "local"
    raise FileNotFoundError(f"No model found")


def run_inference():
    model, version = load_latest_model()
    last_row = read_latest_features()
    X = last_row[FEATURES].values

    prediction = model.predict(X)[0]

    today = date.today()
    from dateutil.relativedelta import relativedelta
    target_month = (today.replace(day=1) + relativedelta(months=1))

    pred_df = pd.DataFrame([{
        "prediction_date": today,
        "target_month": target_month,
        "model_name": "random_forest",
        "model_version": str(version),
        "predicted_value": round(prediction, 2),
        "actual_value": None,
        "upper_bound": round(prediction * 1.1, 2),
        "lower_bound": round(prediction * 0.9, 2),
    }])

    save_predictions_to_postgres(pred_df)
    print(f"Prediction for {target_month}: {prediction:.2f}")

    try:
        from scripts.telegram_alert import send_business_alert
        if prediction > 1500000:
            send_business_alert("Gold Price Prediction", prediction, 1500000, "above")
    except Exception:
        pass


if __name__ == "__main__":
    run_inference()
