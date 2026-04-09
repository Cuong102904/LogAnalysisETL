from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Consume Kafka and write parquet to MinIO (S3A).")
    p.add_argument("--bootstrap-servers", default="broker1:29092")
    p.add_argument("--topic", default="lsp.canonical.events")
    p.add_argument("--s3a-dest", default="s3a://bronze/lsp/bronze/canonical_events")
    p.add_argument("--checkpoint", default="s3a://bronze/lsp/checkpoints/canonical_events")
    p.add_argument("--starting-offsets", default="latest", choices=("earliest", "latest"))
    return p.parse_args()


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str, starting_offsets: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def transform(raw: DataFrame) -> DataFrame:
    # value is bytes -> string JSON
    df = raw.select(
        F.col("timestamp").alias("kafka_ts"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value_json"),
    )

    # Keep as raw JSON string for now; downstream can parse to schema when stabilized
    df = df.withColumn("dt", F.to_date("kafka_ts")).withColumn("hour", F.hour("kafka_ts"))
    return df


def write_to_minio(df: DataFrame, dest: str, checkpoint: str) -> None:
    (
        df.writeStream.format("parquet")
        .option("path", dest)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .partitionBy("dt", "hour")
        .start()
        .awaitTermination()
    )


def main() -> None:
    args = parse_args()
    spark = build_spark("lsp_stream_kafka_to_minio")

    raw = read_kafka_stream(
        spark=spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
    )
    df = transform(raw)
    write_to_minio(df, args.s3a_dest, args.checkpoint)


if __name__ == "__main__":
    main()

