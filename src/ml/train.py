import os
import uuid
import joblib
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
try:
    from src.ml.utils import read_dataset, save_predictions_to_postgres
except ImportError:
    from ml.utils import read_dataset, save_predictions_to_postgres

import mlflow
import mlflow.sklearn

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
TARGET = "target_gold_rp_bulan_depan"

MODELS = {
    "linear_regression": LinearRegression(),
    "random_forest": RandomForestRegressor(
        n_estimators=300,
        max_depth=5,
        min_samples_split=4,
        min_samples_leaf=2,
        random_state=42,
    ),
}


def train_model(model_name: str, model, X_train, y_train, X_test, y_test):
    run_id = str(uuid.uuid4())[:8]

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mape = np.mean(np.abs((y_test - y_pred) / (y_test + 1e-10))) * 100

    print(f"[{model_name}] MAE={mae:.4f}, RMSE={rmse:.4f}, MAPE={mape:.2f}%")

    os.makedirs(MODELS_DIR, exist_ok=True)
    fpath = os.path.join(MODELS_DIR, f"{model_name}.pkl")
    joblib.dump(model, fpath)
    print(f"Model saved to {fpath}")

    with mlflow.start_run(run_name=f"{model_name}_{run_id}"):
        mlflow.log_params(model.get_params())
        mlflow.log_metrics({"mae": mae, "rmse": rmse, "mape": mape})
        mlflow.sklearn.log_model(
            model, artifact_path="model",
            registered_model_name=MLFLOW_MODEL_NAME,
        )
        print(f"Model registered to MLflow: {MLFLOW_MODEL_NAME}")

    return model, y_pred, run_id


def main():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    experiment = mlflow.get_experiment_by_name("gold_price_forecasting")
    if experiment is None:
        mlflow.create_experiment("gold_price_forecasting")
    mlflow.set_experiment("gold_price_forecasting")

    df = read_dataset()

    df = df.dropna(subset=FEATURES + [TARGET])
    X = df[FEATURES].values
    y = df[TARGET].values

    split_idx = int(len(df) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    results = {}
    for name, model in MODELS.items():
        trained_model, y_pred, run_id = train_model(name, model, X_train, y_train, X_test, y_test)
        results[name] = {"model": trained_model, "predictions": y_pred}

        pred_df = pd.DataFrame({
            "prediction_date": pd.Timestamp.today().date(),
            "target_month": pd.to_datetime(df.iloc[split_idx:]["month_key"].values + "-01").date,
            "model_name": name,
            "model_version": run_id,
            "predicted_value": y_pred,
            "actual_value": y_test,
        })
        save_predictions_to_postgres(pred_df)

    print("Training complete. Models logged to MLflow.")


if __name__ == "__main__":
    main()
