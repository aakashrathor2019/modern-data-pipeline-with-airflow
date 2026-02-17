from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import os
import pandas as pd

RAW_BASE = "/home/my/yelp_data_engineering_project/data/raw"
SILVER_BASE = "/home/my/yelp_data_engineering_project/data/silver"

def transform_business():
    raw_folder = os.path.join(RAW_BASE, "business")
    silver_folder = os.path.join(SILVER_BASE, "business")
    os.makedirs(silver_folder, exist_ok=True)
    
    all_data = []
    for file in os.listdir(raw_folder):
        if file.endswith(".json"):
            with open(os.path.join(raw_folder, file), "r") as f:
                data = json.loads(f)
                all_data.append(data)
    
    df = pd.DataFrame(all_data)
    df.to_parquet(os.path.join(silver_folder, "business.parquet"), index=False)

def transform_review():
    raw_folder = os.path.join(RAW_BASE, "review")
    silver_folder = os.path.join(SILVER_BASE, "review")
    os.makedirs(silver_folder, exist_ok=True)
    
    all_data = []
    for file in os.listdir(raw_folder):
        if file.endswith(".json"):
            with open(os.path.join(raw_folder, file), "r") as f:
                data = json.loads(f)
                all_data.append(data)
    
    df = pd.DataFrame(all_data)
    df.to_parquet(os.path.join(silver_folder, "review.parquet"), index=False)

def transform_user():
    raw_folder = os.path.join(RAW_BASE, "user")
    silver_folder = os.path.join(SILVER_BASE, "user")
    os.makedirs(silver_folder, exist_ok=True)
    
    all_data = []
    for file in os.listdir(raw_folder):
        if file.endswith(".json"):
            with open(os.path.join(raw_folder, file), "r") as f:
                data = json.loads(f)
                all_data.append(data)
    
    df = pd.DataFrame(all_data)
    df.to_parquet(os.path.join(silver_folder, "user.parquet"), index=False)

# Define DAG
with DAG(
    "silver_transformation_dag",
    start_date=datetime(2026, 2, 13),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    business_task = PythonOperator(
        task_id="transform_business",
        python_callable=transform_business
    )

    review_task = PythonOperator(
        task_id="transform_review",
        python_callable=transform_review
    )

    user_task = PythonOperator(
        task_id="transform_user",
        python_callable=transform_user
    )

    # Run tasks in parallel
    [business_task, review_task, user_task]
