from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


default_args = {
    "owner": "airflow",
    "retries": 2
}


with DAG(
    "kafka_ingestion",
    default_args=default_args,
    start_date=datetime(2026,2,11),
    schedule_interval=None,
    catchup=False
) as dag:

    ingest = BashOperator(
        task_id="publish_to_kafka",
        bash_command="python3 /home/my/yelp_data_engineering_project/kafka/producer.py"
    )
