from __future__ import annotations

import os

from airflow.models import Variable


class variables:
    spark_master = Variable.get(
        "spark_master", default_var=os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    )
    spark_app_path = Variable.get(
        "spark_app_path", default_var=os.getenv("SPARK_APP_PATH", "/opt/project")
    )
    spark_default_packages = Variable.get(
        "spark_default_packages",
        default_var=os.getenv(
            "SPARK_JARS_PACKAGES",
            (
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            ),
        ),
    )
    s3_endpoint = Variable.get(
        "s3_endpoint", default_var=os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    )
    s3_access_key_id = Variable.get(
        "s3_access_key_id",
        default_var=os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio")),
    )
    s3_secret_access_key = Variable.get(
        "s3_secret_access_key",
        default_var=os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123456")),
    )
