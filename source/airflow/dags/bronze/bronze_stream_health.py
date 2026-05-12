from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from tasks.bronze.runtime_checks import check_file_health, check_kafka_ready, verify_bronze_progress

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
}


with DAG(
    dag_id="bronze_stream_health",
    description="Operational health checks for Kafka to Bronze stream",
    start_date=datetime(2026, 1, 1),
    schedule=os.getenv("BRONZE_HEALTH_SCHEDULE", "*/10 * * * *"),
    catchup=False,
    tags=["bronze", "streaming", "health"],
    default_args=DEFAULT_ARGS,
) as bronze_stream_health_dag:
    start = EmptyOperator(task_id="start")
    check_kafka = PythonOperator(task_id="check_kafka_ready", python_callable=check_kafka_ready)
    verify_stream = PythonOperator(
        task_id="verify_bronze_progress", python_callable=verify_bronze_progress
    )
    check_quality = PythonOperator(task_id="check_file_health", python_callable=check_file_health)
    end = EmptyOperator(task_id="end")
    start >> check_kafka >> verify_stream >> check_quality >> end
