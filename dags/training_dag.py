from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from alert_utils import alert_on_failure, alert_on_success

default_args = {
    "owner": "ipbd-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
    "on_success_callback": alert_on_success,
}

with DAG(
    dag_id="ml_training_pipeline",
    default_args=default_args,
    description="ML training with MLflow tracking",
    schedule_interval="0 9 * * 1",
    start_date=days_ago(1),
    catchup=False,
    tags=["ml", "training"],
) as dag:

    train_models = BashOperator(
        task_id="train_ml_models",
        bash_command="docker exec ipbd-airflow-webserver python -m src.ml.train",
    )

    run_inference = BashOperator(
        task_id="run_inference",
        bash_command="docker exec ipbd-airflow-webserver python -m src.ml.inference",
    )

    train_models >> run_inference
