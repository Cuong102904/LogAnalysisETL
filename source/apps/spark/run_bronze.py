from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from apps.spark.common import load_profile, read_records
from learnlake.connectors import read_kafka_stream
from learnlake.ingestion import build_bronze_records, write_bronze_records
from learnlake.ingestion.envelope import build_bronze_envelope
from learnlake.runtime import build_spark

BRONZE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("source_id", StringType(), False),
        StructField("source_type", StringType(), False),
        StructField("source_event_type", StringType(), True),
        StructField("event_time_raw", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("ingestion_time", TimestampType(), False),
        StructField("raw_payload", StringType(), False),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("schema_version", StringType(), False),
        StructField("processing_date", DateType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LearnLake Bronze ingestion.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--stream", action="store_true", help="Run Kafka-to-Delta streaming mode.")
    return parser.parse_args()


def _spark_bronze_record(record: dict[str, Any]) -> dict[str, Any]:
    converted = dict(record)
    converted["raw_payload"] = json.dumps(converted["raw_payload"], sort_keys=True, default=str)
    return converted


def _write_delta_batch(
    spark: SparkSession,
    records: list[dict[str, Any]],
    output_path: str,
    partition_by: list[str],
) -> None:
    if not records:
        return
    df = spark.createDataFrame([_spark_bronze_record(record) for record in records], BRONZE_SCHEMA)
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(output_path)
def run_stream() -> int:
    args = parse_args()
    profile = load_profile(args.source)
    if profile.input.mode != "kafka":
        raise ValueError("Streaming Bronze mode requires a kafka input profile or Kafka env overrides")
    if not profile.input.bootstrap_servers or not profile.input.topic:
        raise ValueError("Kafka bootstrap servers and topic are required for Bronze streaming")
    if not profile.bronze.checkpoint:
        raise ValueError("Bronze checkpoint path is required for streaming")

    spark = build_spark("learnlake_bronze_ingestion")
    source_df = read_kafka_stream(
        spark,
        profile.input.bootstrap_servers,
        profile.input.topic,
        profile.input.starting_offsets,
        profile.input.max_offsets_per_trigger,
    ).selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "topic AS kafka_topic",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
    )

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        payloads: list[dict[str, Any]] = []
        total_written = 0
        for row in batch_df.toLocalIterator():
            payload = json.loads(row.raw_json)
            envelope = build_bronze_envelope(
                payload,
                profile,
                ingestion_time=datetime.now(timezone.utc),
                kafka_topic=row.kafka_topic,
                kafka_partition=row.kafka_partition,
                kafka_offset=row.kafka_offset,
            )
            payloads.append(envelope.as_record())
            if len(payloads) >= 500:
                _write_delta_batch(
                    spark,
                    payloads,
                    args.output or profile.bronze.path,
                    profile.bronze.partition_by,
                )
                total_written += len(payloads)
                payloads.clear()
        if payloads:
            _write_delta_batch(
                spark,
                payloads,
                args.output or profile.bronze.path,
                profile.bronze.partition_by,
            )
            total_written += len(payloads)
        if total_written:
            print(f"learnlake bronze batch_id={batch_id} wrote {total_written} records", flush=True)

    writer = (
        source_df.writeStream.option("checkpointLocation", profile.bronze.checkpoint)
        .foreachBatch(process_batch)
        .queryName("learnlake_bronze_ingestion")
    )
    if profile.input.trigger_processing_time:
        writer = writer.trigger(processingTime=profile.input.trigger_processing_time)
    writer.start().awaitTermination()
    return 0


def main() -> int:
    args = parse_args()
    if args.stream:
        return run_stream()
    profile = load_profile(args.source)
    input_path = args.input or profile.input.path
    if not input_path:
        raise ValueError("Input path is required for file or fixture Bronze ingestion")
    output_path = args.output or profile.bronze.path
    payloads = read_records(input_path)
    records = build_bronze_records(
        payloads,
        profile,
        ingestion_time=datetime.now(timezone.utc),
    )
    write_bronze_records(output_path, records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
