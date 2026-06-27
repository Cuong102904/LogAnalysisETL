from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from tasks.spark_task import spark_task
from utils.catalog import bronze_table_path

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
}


def _enabled(env_name: str, default: str = "true") -> bool:
    return os.getenv(env_name, default).lower() == "true"


def _maintenance_arguments() -> list[str]:
    arguments = [
        "--table-path",
        bronze_table_path(),
        "--app-name",
        os.getenv("BRONZE_MAINTENANCE_APP_NAME", "bronze_delta_maintenance"),
    ]
    if _enabled("BRONZE_OPTIMIZE_ENABLED"):
        arguments.append("--optimize")
    if _enabled("BRONZE_VACUUM_ENABLED", default="false"):
        arguments.extend(
            [
                "--vacuum",
                "--vacuum-retention-hours",
                os.getenv("BRONZE_VACUUM_RETENTION_HOURS", "168"),
            ]
        )
    return arguments


with DAG(
    dag_id="bronze_table_maintenance",
    description="Scheduled OPTIMIZE and VACUUM for Bronze Delta table",
    start_date=datetime(2026, 1, 1),
    schedule=os.getenv("BRONZE_MAINTENANCE_SCHEDULE", "0 0 * * *"),
    catchup=False,
    tags=["bronze", "maintenance", "delta"],
    default_args=DEFAULT_ARGS,
) as bronze_table_maintenance_dag:
    start = EmptyOperator(task_id="start")
    run_maintenance = spark_task(
        name="run_bronze_maintenance",
        command="apps/maintenance",
        subcommand="delta_maintenance",
        pool="bronze_pool",
        arguments=_maintenance_arguments(),
        execution_timeout=timedelta(hours=2),
        spark_driver_memory=os.getenv("BRONZE_MAINTENANCE_DRIVER_MEMORY", "512m"),
        spark_executor_memory=os.getenv("BRONZE_MAINTENANCE_EXECUTOR_MEMORY", "1g"),
        spark_executor_cores=int(os.getenv("BRONZE_MAINTENANCE_EXECUTOR_CORES", "1")),
    )
    end = EmptyOperator(task_id="end")
    start >> run_maintenance >> end
