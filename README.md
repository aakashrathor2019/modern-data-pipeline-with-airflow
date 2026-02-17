# Yelp Data Engineering Project

## Architecture
Bronze → Silver → Gold (Medallion Architecture)
Kafka → Databricks → Airflow

## How to Run
1. Start Kafka server
2. Run kafka/producer.py
3. Run Databricks notebooks
4. Trigger Airflow DAGs: kafka_ingestion, silver_transformation, gold_aggregation

## Assumptions
- Kafka at localhost:9092
- Delta tables stored at /mnt/delta
- Databricks cluster exists
- Airflow is configured with Databricks Operator

## Validation
- Check review counts and avg ratings between Silver and Gold
- Validate city metrics aggregation