import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    minio_user = os.getenv("MINIO_ROOT_USER", "minio")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minio123456")

    return (
        SparkSession.builder.master("local[*]").appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "io.delta:delta-spark_2.12:3.2.0",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                ]
            ),
        )
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .getOrCreate()
    )
