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
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "dag-65",
        "namespace": "default"
    },
    "spec": {
        "type": "Scala",
        "mode": "cluster",
        "image": "docker.io/channnuu/chandan_spark:3.5.2",
        "imagePullPolicy": "IfNotPresent",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.2.jar",
        "sparkVersion": "3.1.2",
        "restartPolicy": {
            "type": "Never"
        },
        "driver": {
            "cores": 1,
            "memory": "512m",
            "serviceAccount": "spark",
            "env": [
                {
                    "name": "SPARK_DRIVER_MEMORY",
                    "value": "512m"
                }
            ]
        },
        "executor": {
            "cores": 1,
            "instances": 2,
            "memory": "512m",
        }
    }
}

dag = DAG(
    "dag_65",  # same name as application, and there should be no underscore in application name
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow",
    schedule_interval=timedelta(days=1),  # Changed 'schedule' to 'schedule_interval'
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = SparkKubernetesOperator(
    task_id="submit_dag_65",  # NOTE: Ensure this task ID is correct
    executor_config={
        "env": {
            "KUBERNETES_OPERATOR_LOG_LEVEL": "DEBUG",
        },
    },
    namespace="default",  # ok
    image="bitnami/kubectl:latest",  # ok
    template_spec=spec,
    get_logs=True,  # ok
    delete_on_termination=False,  # new
    dag=dag,
    # do_xcom_push=True,  # Uncomment if you need to push logs to XCom
    # cmds=["kubectl", "apply", "-f", "https://vishalsparklogs.blob.core.windows.net/spark-logs/yaml/sparktest8.yaml"],
    # is_delete_operator_pod=False,
)
