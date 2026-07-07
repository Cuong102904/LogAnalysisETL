from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from apps.spark.common import load_profile
from learnlake.ingestion.bronze_transform import (
    transform_bronze_dataframe,
)
from learnlake.connectors import read_kafka_stream
from learnlake.runtime import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LearnLake Bronze ingestion.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--output")
    return parser.parse_args()


def _read_kafka_lines(
    spark: SparkSession,
    *,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str,
    max_offsets_per_trigger: int | None,
) -> DataFrame:
    return read_kafka_stream(
        spark,
        bootstrap_servers,
        topic,
        starting_offsets,
        max_offsets_per_trigger,
    ).selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "topic AS kafka_topic",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
    )


def _run_stream(args: argparse.Namespace) -> int:
    profile = load_profile(args.source)
    if profile.input.mode != "kafka":
        raise ValueError("Streaming Bronze mode requires a kafka input profile or Kafka env overrides")
    if not profile.input.bootstrap_servers:
        raise ValueError("Kafka bootstrap servers are required for Bronze streaming")
    if not profile.input.topic:
        raise ValueError("Kafka topic is required for Bronze streaming")
    if not profile.bronze.checkpoint:
        raise ValueError("Bronze checkpoint path is required for streaming")

    spark = build_spark("learnlake_bronze_ingestion")
    source_df = _read_kafka_lines(
        spark,
        bootstrap_servers=profile.input.bootstrap_servers,
        topic=profile.input.topic,
        starting_offsets=profile.input.starting_offsets,
        max_offsets_per_trigger=profile.input.max_offsets_per_trigger,
    )
    bronze_df = transform_bronze_dataframe(source_df, profile)

    output_path = args.output or profile.bronze.path

    writer = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", profile.bronze.checkpoint)
        .option("path", output_path)
        .queryName("learnlake_bronze_ingestion")
        .trigger(availableNow=True)
    )
    if profile.bronze.partition_by:
        writer = writer.partitionBy(*profile.bronze.partition_by)
    writer.start().awaitTermination()
    return 0


def main() -> int:
    args = parse_args()
    return _run_stream(args)


if __name__ == "__main__":
    raise SystemExit(main())
