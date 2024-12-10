from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime, timedelta
import os
import requests

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "dag-77-pod",
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Function to download the YAML file
def download_yaml_file(url, target_path):
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    with open(target_path, "wb") as f:
        f.write(response.content)

# Path to save the YAML file
yaml_file_path = "/tmp/sparktest8.yaml"

# Download the YAML file
download_yaml_file("https://vishalsparklogs.blob.core.windows.net/spark-logs/yaml/sparktest8.yaml", yaml_file_path)

# Define the task
submit_spark_job = SparkKubernetesOperator(
    task_id="submit-dag-77-pod",
    name="dag-77-pod-spark-job",
    namespace="default",
    image="bitnami/kubectl:latest",
    application_file=yaml_file_path,  # Local file path
    do_xcom_push=True,
    get_logs=True,
    delete_on_termination=False,
    dag=dag,
)
