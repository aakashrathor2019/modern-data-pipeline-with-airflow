from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime


default_args = {"owner":"airflow","retries":2}


with DAG(
    "validation_pipeline",
    default_args=default_args,
    start_date=datetime(2026,2,11),
    schedule_interval=None,
    catchup=False
) as dag:


    validate = DatabricksSubmitRunOperator(
        task_id="run_validation",
        new_cluster={},
        notebook_task={"notebook_path":"databricks/notebooks/04_validation.sql"}
    )
