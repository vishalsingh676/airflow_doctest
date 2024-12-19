from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.base import BaseSensorOperator
from kubernetes import client, config
from datetime import datetime, timedelta
import subprocess
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


class SparkAppSensor(BaseSensorOperator):
    def _init_(self, namespace, task_id_prefix, **kwargs):
        super()._init_(**kwargs)
        self.namespace = namespace
        self.task_id_prefix = task_id_prefix

    def poke(self, context):
        # Load Kubernetes config
        config.load_kube_config()
        custom_api = client.CustomObjectsApi()

        # Fetch all SparkApplications in the namespace
        apps = custom_api.list_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=self.namespace,
            plural="sparkapplications"
        )

        # Find the SparkApplication matching the task_id_prefix
        for app in apps.get("items", []):
            name = app["metadata"]["name"]
            if name.startswith(self.task_id_prefix):
                state = app["status"]["applicationState"]["state"]
                self.log.info(f"Found SparkApplication {name} with state {state}")
                return state == "COMPLETED"

        # If no matching application is found, keep waiting
        self.log.info(f"No SparkApplication found for prefix: {self.task_id_prefix}")
        return False


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
    "dag_59", #same name as application. and there should be no underscore in application name
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow",
    # schedule=timedelta(days=1),
    # start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = SparkKubernetesOperator(
    task_id="submit_spark_dag_59", #NOT but in blog
    executor_config={
        "env": {
            "KUBERNETES_OPERATOR_LOG_LEVEL": "DEBUG",
        },
    },
    namespace="default", #ok
    image="docker.io/channnuu/chandan_spark:3.5.2", #ok
    template_spec=spec,
    get_logs=True, #ok
    delete_on_termination=False, #new
    dag=dag,
    # do_xcom_push=True,
    # cmds=["kubectl", "apply", "-f", "https://vishalsparklogs.blob.core.windows.net/spark-logs/yaml/sparktest8.yaml"],
    # is_delete_operator_pod=False,
)


wait_for_spark_app = SparkAppSensor(
    task_id="wait_for_spark_app",
    namespace="default",
    task_id_prefix="submit-dag-64",  # Matches the SparkApplication name prefix
    poke_interval=30,  # Check every 30 seconds
    timeout=3600,      # Timeout after 1 hour
    dag=dag,
)

submit_spark_job >> wait_for_spark_app
