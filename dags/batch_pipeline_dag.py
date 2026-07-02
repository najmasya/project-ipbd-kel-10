import sys
sys.path.insert(0, "/opt/airflow/src")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from alert_utils import alert_on_failure, alert_on_success

default_args = {
    "owner": "ipbd-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
    "on_success_callback": alert_on_success,
}

PYTHON_EXEC = "docker exec ipbd-airflow-webserver python /opt/airflow/src/batch"

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    description="Batch ingestion pipeline: inflasi, kurs, emas",
    schedule_interval="0 7 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["batch", "ingestion"],
) as dag:

    ingest_inflasi = BashOperator(
        task_id="ingest_inflasi",
        bash_command=f"{PYTHON_EXEC}/ingest_inflasi.py",
    )

    ingest_kurs = BashOperator(
        task_id="ingest_kurs",
        bash_command=f"{PYTHON_EXEC}/ingest_kurs.py",
    )

    ingest_emas = BashOperator(
        task_id="ingest_emas",
        bash_command=f"{PYTHON_EXEC}/ingest_emas.py",
    )

    log_success = BashOperator(
        task_id="log_pipeline_success",
        bash_command='echo "Batch pipeline completed at $(date)" >> /opt/airflow/logs/pipeline_execution.log',
    )

    [ingest_inflasi, ingest_kurs, ingest_emas] >> log_success
