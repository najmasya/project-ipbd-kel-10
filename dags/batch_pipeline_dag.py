from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    description="Batch ingestion pipeline: inflasi, kurs, emas",
    schedule_interval="0 7 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["batch", "ingestion"],
) as dag:

    ingest_inflasi = PythonOperator(
        task_id="ingest_inflasi",
        python_callable=__import__("src.batch.ingest_inflasi", fromlist=["ingest_inflasi"]).ingest_inflasi,
        op_kwargs={"year": datetime.now().year},
    )

    ingest_kurs = PythonOperator(
        task_id="ingest_kurs",
        python_callable=__import__("src.batch.ingest_kurs", fromlist=["ingest_kurs_usd_idr"]).ingest_kurs_usd_idr,
    )

    ingest_emas = PythonOperator(
        task_id="ingest_emas",
        python_callable=__import__("src.batch.ingest_emas", fromlist=["ingest_gold_price"]).ingest_gold_price,
    )

    log_success = BashOperator(
        task_id="log_pipeline_success",
        bash_command='echo "Batch pipeline completed at $(date)" >> /opt/airflow/logs/pipeline_execution.log',
    )

    [ingest_inflasi, ingest_kurs, ingest_emas] >> log_success
