import os
import sys

from pyspark.sql import SparkSession


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def build_spark(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    master_url = os.getenv("SPARK_MASTER_URL")
    if master_url:
        builder = builder.master(master_url)

    builder = builder.config("spark.pyspark.driver.python", sys.executable)

    jars_packages = os.getenv("SPARK_JARS_PACKAGES")
    if jars_packages:
        builder = builder.config("spark.jars.packages", jars_packages)

    s3_endpoint = _first_env("S3A_ENDPOINT", "MINIO_ENDPOINT")
    if s3_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)

    s3_access_key = _first_env("AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY", "MINIO_ROOT_USER")
    if s3_access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", s3_access_key)

    s3_secret_key = _first_env("AWS_SECRET_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_ROOT_PASSWORD")
    if s3_secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)

    s3_path_style = _first_env("S3A_PATH_STYLE_ACCESS", "MINIO_PATH_STYLE_ACCESS")
    if s3_path_style:
        builder = builder.config("spark.hadoop.fs.s3a.path.style.access", s3_path_style)

    s3_ssl_enabled = _first_env("S3A_SSL_ENABLED", "MINIO_SSL_ENABLED")
    if s3_ssl_enabled:
        builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3_ssl_enabled)

    return builder.getOrCreate()
