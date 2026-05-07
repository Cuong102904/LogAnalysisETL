from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.config import Config
from utils.env import env_float, env_int

from kafka import KafkaConsumer, TopicPartition


def _s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minio123456")
    region = os.getenv("MINIO_REGION", "us-east-1")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )


def _read_topic_offset(topic: str) -> int:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:29092,broker2:29092,broker3:29092")
    consumer = KafkaConsumer(
        bootstrap_servers=[s.strip() for s in bootstrap.split(",") if s.strip()],
        enable_auto_commit=False,
        group_id=None,
        request_timeout_ms=10000,
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"Topic {topic} is missing or has no partitions")
        topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
        end_offsets = consumer.end_offsets(topic_partitions, timeout=10)
        return int(sum(end_offsets.values()))
    finally:
        consumer.close()


def check_kafka_ready() -> dict[str, Any]:
    topic = os.getenv("KAFKA_TOPIC_RAW", "mooc.raw.events")
    probe_seconds = env_int("OFFSET_PROBE_SECONDS", 10)
    min_delta = env_int("KAFKA_OFFSET_MIN_DELTA", 1)
    start_offset = _read_topic_offset(topic)
    time.sleep(probe_seconds)
    end_offset = _read_topic_offset(topic)
    delta = end_offset - start_offset
    if delta < min_delta:
        raise RuntimeError(
            f"Kafka offset did not progress enough for {topic}. start={start_offset}, end={end_offset}, delta={delta}, required>={min_delta}"
        )
    return {"topic": topic, "start_offset": start_offset, "end_offset": end_offset, "delta": delta}


def _list_numeric_checkpoint_keys() -> list[int]:
    s3 = _s3_client()
    bucket = os.getenv("CHECKPOINT_BUCKET", "checkpoints")
    prefix = os.getenv("CHECKPOINT_PREFIX", "mooc/bronze_ingestor/offsets/")
    paginator = s3.get_paginator("list_objects_v2")
    numeric_ids: list[int] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            suffix = key.removeprefix(prefix).strip("/")
            if suffix.isdigit():
                numeric_ids.append(int(suffix))
    return sorted(numeric_ids)


def verify_bronze_progress() -> dict[str, Any]:
    probe_seconds = env_int("OFFSET_PROBE_SECONDS", 10)
    min_delta = env_int("CHECKPOINT_MIN_DELTA", 1)
    before = _list_numeric_checkpoint_keys()
    before_max = before[-1] if before else -1
    time.sleep(probe_seconds)
    after = _list_numeric_checkpoint_keys()
    after_max = after[-1] if after else -1
    delta = after_max - before_max
    if delta < min_delta:
        raise RuntimeError(
            f"Checkpoint did not progress. before={before_max}, after={after_max}, delta={delta}, required>={min_delta}"
        )
    return {"checkpoint_before": before_max, "checkpoint_after": after_max, "delta": delta}


def check_file_health() -> dict[str, Any]:
    s3 = _s3_client()
    bucket = os.getenv("BRONZE_TABLE_BUCKET", "bronze")
    prefix = os.getenv("BRONZE_TABLE_PREFIX", "mooc/bronze/mooc_events_raw/")
    small_file_bytes = env_int("BRONZE_SMALL_FILE_BYTES", 1_048_576)
    file_count_warn = env_int("BRONZE_FILE_COUNT_WARN", 5000)
    small_ratio_warn = env_float("BRONZE_SMALL_FILE_RATIO_WARN", 0.7)
    checkpoint_stale_seconds = env_int("CHECKPOINT_STALE_SECONDS", 300)

    paginator = s3.get_paginator("list_objects_v2")
    parquet_sizes: list[int] = []
    latest_write_ts: datetime | None = None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            if key.endswith(".parquet"):
                parquet_sizes.append(int(obj.get("Size", 0)))
                lm = obj.get("LastModified")
                if isinstance(lm, datetime) and (latest_write_ts is None or lm > latest_write_ts):
                    latest_write_ts = lm

    file_count = len(parquet_sizes)
    if file_count == 0:
        raise RuntimeError("No parquet files found in Bronze table path")
    small_files = sum(1 for size in parquet_sizes if size <= small_file_bytes)
    small_ratio = small_files / file_count
    avg_size = sum(parquet_sizes) / file_count

    if file_count > file_count_warn:
        raise RuntimeError(
            f"Bronze file_count={file_count} exceeded warning threshold={file_count_warn}"
        )
    if small_ratio > small_ratio_warn:
        raise RuntimeError(
            f"Bronze small-file ratio={small_ratio:.2f} exceeded threshold={small_ratio_warn:.2f} "
            f"(<= {small_file_bytes} bytes)."
        )

    checkpoint_bucket = os.getenv("CHECKPOINT_BUCKET", "checkpoints")
    checkpoint_prefix = os.getenv("CHECKPOINT_PREFIX", "mooc/bronze_ingestor/offsets/")
    checkpoint_listing = s3.list_objects_v2(
        Bucket=checkpoint_bucket, Prefix=checkpoint_prefix, MaxKeys=1000
    )
    latest_checkpoint_ts: datetime | None = None
    for obj in checkpoint_listing.get("Contents", []):
        lm = obj.get("LastModified")
        if isinstance(lm, datetime) and (latest_checkpoint_ts is None or lm > latest_checkpoint_ts):
            latest_checkpoint_ts = lm
    if latest_checkpoint_ts is None:
        raise RuntimeError("Checkpoint offsets are missing")

    now = datetime.now(timezone.utc)
    stale_seconds = int((now - latest_checkpoint_ts).total_seconds())
    if stale_seconds > checkpoint_stale_seconds:
        raise RuntimeError(
            f"Checkpoint appears stale. stale_seconds={stale_seconds}, threshold={checkpoint_stale_seconds}"
        )

    return {
        "file_count": file_count,
        "small_files": small_files,
        "small_ratio": round(small_ratio, 4),
        "avg_file_size_bytes": int(avg_size),
        "latest_bronze_write": latest_write_ts.isoformat() if latest_write_ts else None,
        "latest_checkpoint": latest_checkpoint_ts.isoformat(),
        "checkpoint_stale_seconds": stale_seconds,
    }
