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

GOLD_TABLES = [
    "video_friction_signals",
    "pdf_engagement_features",
    "quiz_attempt_metrics",
    "user_learning_profile_daily",
    "exam_integrity_signals",
    "behavior_anomaly_signals",
    "anomaly_alerts",
]


def _enabled(env_name: str, default: str = "true") -> bool:
    return os.getenv(env_name, default).lower() == "true"


def _maintenance_arguments(table_name: str) -> list[str]:
    base_path = os.getenv("GOLD_TABLE_BASE_PATH", "s3a://lakehouse/mooc/gold")
    table_path = f"{base_path}/{table_name}"

    arguments = [
        "--table-path",
        table_path,
        "--app-name",
        f"gold_{table_name}_maintenance",
    ]
    if _enabled("GOLD_OPTIMIZE_ENABLED"):
        arguments.append("--optimize")
    if _enabled("GOLD_VACUUM_ENABLED", default="false"):
        arguments.extend(
            [
                "--vacuum",
                "--vacuum-retention-hours",
                os.getenv("GOLD_VACUUM_RETENTION_HOURS", "168"),
            ]
        )
    return arguments


with DAG(
    dag_id="gold_maintenance_behavior_tables",
    description="Scheduled OPTIMIZE and VACUUM for Gold behavior tables",
    start_date=datetime(2026, 1, 1),
    schedule=os.getenv("GOLD_MAINTENANCE_SCHEDULE", "15 1 * * *"),
    catchup=False,
    tags=["gold", "maintenance", "delta", "behavior"],
    default_args=DEFAULT_ARGS,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    previous = start
    for table_name in GOLD_TABLES:
        task = spark_task(
            name=f"run_{table_name}_maintenance",
            command="apps/maintenance",
            subcommand="delta_maintenance",
            pool="gold_pool",
            arguments=_maintenance_arguments(table_name),
            execution_timeout=timedelta(hours=1),
            spark_driver_memory=os.getenv("GOLD_MAINTENANCE_DRIVER_MEMORY", "512m"),
            spark_executor_memory=os.getenv("GOLD_MAINTENANCE_EXECUTOR_MEMORY", "1g"),
            spark_executor_cores=int(os.getenv("GOLD_MAINTENANCE_EXECUTOR_CORES", "1")),
        )
        previous >> task
        previous = task

    previous >> end
