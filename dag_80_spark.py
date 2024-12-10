from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
# from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import subprocess
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dag-80-pod", #same name as application. and there should be no underscore in application name
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = SparkKubernetesOperator(
    task_id="submit-dag-80-pod", #NOT but in blog
    name="submit-dag-80-pod", #ok
    namespace="default", #ok
    image="bitnami/kubectl:latest", #ok
    application_file="https://vishalsparklogs.blob.core.windows.net/spark-logs/yaml/sparktest8.yaml",
    do_xcom_push=True,
    get_logs=True, #ok
    delete_on_termination=False, #new
    dag=dag,
    # cmds=["kubectl", "apply", "-f", "https://vishalsparklogs.blob.core.windows.net/spark-logs/yaml/sparktest8.yaml"],
    # is_delete_operator_pod=False,
)
