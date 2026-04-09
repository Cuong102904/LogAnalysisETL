from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="log_stream_pipeline",
    description="Draft DAG: check Kafka -> trigger Spark -> check quality",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["draft", "streaming"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_kafka_ready = EmptyOperator(task_id="check_kafka_ready")
    submit_spark_job = EmptyOperator(task_id="submit_spark_job")
    check_quality = EmptyOperator(task_id="check_quality")

    end = EmptyOperator(task_id="end")

    start >> check_kafka_ready >> submit_spark_job >> check_quality >> end

