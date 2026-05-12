from __future__ import annotations

import os
from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.task.trigger_rule import TriggerRule
from config.variables import variables


def spark_task(
    name: str,
    command: str,
    subcommand: str,
    pool: str,
    arguments: str | list[str] | None = None,
    env: dict[str, str] | None = None,
    trigger_rule: TriggerRule | str = TriggerRule.ALL_SUCCESS,
    execution_timeout: timedelta | None = None,
    retries: int = 1,
    with_packages: bool = True,
    shuffle_partitions: int = 200,
    spark_driver_memory: str = "512m",
    spark_executor_memory: str = "768m",
    spark_executor_cores: int = 1,
) -> SparkSubmitOperator:
    args = [arguments] if isinstance(arguments, str) else list(arguments or [])
    python_bin = os.getenv("PYSPARK_PYTHON", "/home/airflow/.local/bin/python3")

    conf = {
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.hadoop.fs.s3a.endpoint": variables.s3_endpoint,
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": variables.s3_access_key_id,
        "spark.hadoop.fs.s3a.secret.key": variables.s3_secret_access_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.executorEnv.PYTHONPATH": variables.spark_app_path,
        "spark.jars.ivy": os.getenv("SPARK_IVY_DIR", "/home/airflow/.ivy2"),
        "spark.pyspark.python": python_bin,
        "spark.pyspark.driver.python": python_bin,
        "spark.cores.max": "1",
    }

    app_path = f"{variables.spark_app_path}/{command}/{subcommand}.py"

    return SparkSubmitOperator(
        task_id=name,
        conn_id="spark_default",
        application=app_path,
        conf=conf,
        packages=variables.spark_default_packages if with_packages else None,
        application_args=args,
        driver_memory=spark_driver_memory,
        executor_memory=spark_executor_memory,
        executor_cores=spark_executor_cores,
        env_vars={
            "PYTHONPATH": f"/opt/airflow:{variables.spark_app_path}",
            "SPARK_CONF_DIR": os.getenv("SPARK_CONF_DIR", f"{variables.spark_app_path}/conf"),
            "PYSPARK_PYTHON": python_bin,
            "PYSPARK_DRIVER_PYTHON": python_bin,
            **(env or {}),
        },
        trigger_rule=trigger_rule,
        execution_timeout=execution_timeout,
        retries=retries,
        pool=pool,
        verbose=False,
    )
