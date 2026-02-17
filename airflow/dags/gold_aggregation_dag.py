import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

SILVER_BASE = "/home/my/yelp_data_engineering_project/data/silver"
GOLD_BASE = "/home/my/yelp_data_engineering_project/data/gold"

def gold_business_ratings():
    df = pd.read_parquet(os.path.join(SILVER_BASE, "review/review.parquet"))
    gold_folder = os.path.join(GOLD_BASE, "business_ratings")
    os.makedirs(gold_folder, exist_ok=True)
    result = df.groupby("business_id")["stars"].mean().reset_index()
    result.to_csv(os.path.join(gold_folder, "business_ratings.csv"), index=False)

def gold_user_engagement():
    df = pd.read_parquet(os.path.join(SILVER_BASE, "review/review.parquet"))
    gold_folder = os.path.join(GOLD_BASE, "user_engagement")
    os.makedirs(gold_folder, exist_ok=True)
    result = df.groupby("user_id")["review_id"].count().reset_index()
    result.rename(columns={"review_id": "total_reviews"}, inplace=True)
    result.to_csv(os.path.join(gold_folder, "user_engagement.csv"), index=False)

def gold_city_metrics():
    df_review = pd.read_parquet(os.path.join(SILVER_BASE, "review/review.parquet"))
    df_business = pd.read_parquet(os.path.join(SILVER_BASE, "business/business.parquet"))
    gold_folder = os.path.join(GOLD_BASE, "city_metrics")
    os.makedirs(gold_folder, exist_ok=True)
    merged = df_review.merge(df_business, on="business_id", how="left")
    result = merged.groupby("city")["stars"].mean().reset_index()
    result.to_csv(os.path.join(gold_folder, "city_metrics.csv"), index=False)

# DAG definition
with DAG(
    "gold_aggregation_dag",
    start_date=datetime(2026, 2, 13),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_business_ratings = PythonOperator(
        task_id="gold_business_ratings",
        python_callable=gold_business_ratings
    )

    task_user_engagement = PythonOperator(
        task_id="gold_user_engagement",
        python_callable=gold_user_engagement
    )

    task_city_metrics = PythonOperator(
        task_id="gold_city_metrics",
        python_callable=gold_city_metrics
    )

    [task_business_ratings, task_user_engagement, task_city_metrics]
