from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

spec = {
  "apiVersion": "sparkoperator.k8s.io/v1beta2",
  "kind": "SparkApplication",
  "metadata": {
    "name": "dag-59",
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
      "type": "OnFailure",
      "onSubmissionFailureRetries": 3,
      "onSubmissionFailureRetryInterval": 10,
      "onFailureRetries": 3,
      "onFailureRetryInterval": 10
    },
    "sparkConf": {
      "spark.eventLog.enabled": "true",
      "spark.eventLog.dir": "wasb://spark-logs@pocdatabricksdoc.blob.core.windows.net/spark-logs",
      "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
      "spark.hadoop.fs.azure.account.key.pocdatabricksdoc.blob.core.windows.net": "jr63R2MPRqUvSDZ/EtX+sNu32kz2wPVVU9S3wJoXZCx6xNJYI+yesstPOVxoyMuWqX2RmKBZRlZg+AStdYa2HA=="
    },
    "driver": {
      "cores": 1,
      "memory": "512m",
      "serviceAccount": "spark",
      "env": [
        {
          "name": "SPARK_DRIVER_MEMORY",
          "value": "512m"
        },
        {"name": "SPARK_LOG_LEVEL", "value": "DEBUG"}
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
    "dag_59",
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow",
    # schedule=timedelta(days=1),
    # start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = SparkKubernetesOperator(
    task_id="submit_spark_dag_59",
    executor_config={
        "env": {
            "KUBERNETES_OPERATOR_LOG_LEVEL": "DEBUG",
        },
    },
    namespace="default",
    image="docker.io/channnuu/chandan_spark:3.5.2",
    template_spec=spec,
    get_logs=True,
    delete_on_termination=False,
    dag=dag,
)

submit_spark_job
