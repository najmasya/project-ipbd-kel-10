from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from alert_utils import alert_on_failure

SPARK_EXEC = "docker exec ipbd-spark-master spark-submit"
SRC_DIR = "/opt/spark-apps"

SPARK_ARGS = (
    "--master spark://spark-master:7077 "
    "--deploy-mode client "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=minio_admin "
    "--conf spark.hadoop.fs.s3a.secret.key=minio_pass123 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.1 "
)

default_args = {
    "owner": "ipbd-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="processing_pipeline",
    default_args=default_args,
    description="ETL batch to silver + feature engineering via Spark",
    schedule_interval="0 8 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["processing", "etl"],
) as dag:

    etl_batch_to_silver = BashOperator(
        task_id="etl_batch_to_silver",
        bash_command=f"{SPARK_EXEC} {SPARK_ARGS} {SRC_DIR}/processing/etl_batch_to_silver.py",
    )

    feature_engineering = BashOperator(
        task_id="feature_engineering",
        bash_command=f"{SPARK_EXEC} {SPARK_ARGS} {SRC_DIR}/processing/feature_engineering.py",
    )

    etl_batch_to_silver >> feature_engineering
