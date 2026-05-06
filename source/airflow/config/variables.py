from __future__ import annotations

from airflow.models import Variable


class variables:
    spark_master = Variable.get("spark_master", default_var="local[*]")
    spark_app_path = Variable.get("spark_app_path", default_var="/opt/project/spark")
    spark_default_packages = Variable.get(
        "spark_default_packages",
        default_var=(
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
    )
    s3_endpoint = Variable.get("s3_endpoint", default_var="http://minio:9000")
    s3_access_key_id = Variable.get("s3_access_key_id", default_var="minio")
    s3_secret_access_key = Variable.get("s3_secret_access_key", default_var="minio123456")
