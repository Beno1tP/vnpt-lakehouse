"""Task 3 step 3.6: data assets shared by the DAGs (one definition, so producer and consumer cannot drift apart).
LAKE_SILVER is updated by chatbot_bronze.validate_silver (only after Silver passed its checks) and schedules the DAG
trino_reconcile. Not a DAG file: the dag-processor finds no DAG here."""

from airflow.sdk import Asset

LAKE_SILVER = Asset(name="lake_silver", uri="s3://lakehouse/warehouse/silver.db")
