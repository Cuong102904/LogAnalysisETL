from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from tasks.spark_task import spark_task

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
}

def _enabled(env_name: str, default: str = "true") -> bool:
    return os.getenv(env_name, default).lower() == "true"

def _maintenance_arguments() -> list[str]:
    base_path = os.getenv("SILVER_TABLE_BASE_PATH", "s3a://lakehouse/mooc/silver")
    table_path = f"{base_path}/unknown_events"

    arguments = [
        "--table-path",
        table_path,
        "--app-name",
        "silver_unknown_events_maintenance",
    ]
    if _enabled("SILVER_OPTIMIZE_ENABLED"):
        arguments.append("--optimize")
    if _enabled("SILVER_VACUUM_ENABLED", default="false"):
        arguments.extend(
            [
                "--vacuum",
                "--vacuum-retention-hours",
                os.getenv("SILVER_VACUUM_RETENTION_HOURS", "168"),
            ]
        )
    return arguments

with DAG(
    dag_id="silver_maintenance_unknown_events",
    description="Scheduled OPTIMIZE and VACUUM for Silver Delta table: unknown_events",
    start_date=datetime(2026, 1, 1),
    schedule=os.getenv("SILVER_MAINTENANCE_SCHEDULE", "0 1 * * *"),
    catchup=False,
    tags=["silver", "maintenance", "delta", "unknown_events"],
    default_args=DEFAULT_ARGS,
) as dag:

    start = EmptyOperator(task_id="start")

    run_maintenance = spark_task(
        name="run_maintenance",
        command="apps/maintenance",
        subcommand="delta_maintenance",
        pool="silver_pool",
        arguments=_maintenance_arguments(),
        execution_timeout=timedelta(hours=1),
        spark_driver_memory=os.getenv("SILVER_MAINTENANCE_DRIVER_MEMORY", "512m"),
        spark_executor_memory=os.getenv("SILVER_MAINTENANCE_EXECUTOR_MEMORY", "1g"),
        spark_executor_cores=int(os.getenv("SILVER_MAINTENANCE_EXECUTOR_CORES", "1")),
    )

    end = EmptyOperator(task_id="end")

    start >> run_maintenance >> end
