from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from learnlake.contracts import SourceProfile
from learnlake.normalization.values import get_path, parse_timestamp

BRONZE_SCHEMA_VERSION = "bronze-envelope-v1"
RAW_INPUT_COLUMN = "raw_json"


def _stable_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _event_basis(
    *,
    source_id: str,
    event_time_raw: str | None,
    event_type: str | None,
    raw_json: str,
    kafka_topic: str | None,
    kafka_partition: int | None,
    kafka_offset: int | None,
) -> str:
    return "|".join(
        [
            source_id,
            event_time_raw or "",
            event_type or "",
            raw_json,
            str(kafka_topic or ""),
            str(kafka_partition if kafka_partition is not None else ""),
            str(kafka_offset if kafka_offset is not None else ""),
        ]
    )


def build_bronze_record(
    payload: dict[str, Any] | str,
    profile: SourceProfile,
    *,
    ingestion_time: datetime | None = None,
    kafka_topic: str | None = None,
    kafka_partition: int | None = None,
    kafka_offset: int | None = None,
) -> dict[str, Any]:
    observed_ingestion_time = ingestion_time or datetime.now(timezone.utc)
    if isinstance(payload, str):
        raw_json = payload
        payload_object = json.loads(payload)
    else:
        payload_object = dict(payload)
        raw_json = _stable_payload(payload_object)

    event_time_raw_value = get_path(payload_object, profile.input.event_time_field)
    event_time_raw = None if event_time_raw_value is None else str(event_time_raw_value)
    event_type = (
        get_path(payload_object, profile.input.event_type_field)
        if profile.input.event_type_field
        else None
    )
    event_basis = _event_basis(
        source_id=profile.source_id,
        event_time_raw=event_time_raw,
        event_type=None if event_type is None else str(event_type),
        raw_json=raw_json,
        kafka_topic=kafka_topic,
        kafka_partition=kafka_partition,
        kafka_offset=kafka_offset,
    )
    event_id = hashlib.sha256(event_basis.encode("utf-8")).hexdigest()
    return {
        "event_id": event_id,
        "source_id": profile.source_id,
        "source_type": profile.source_type,
        "source_event_type": None if event_type is None else str(event_type),
        "event_time_raw": event_time_raw,
        "event_time": parse_timestamp(event_time_raw),
        "ingestion_time": observed_ingestion_time,
        "raw_payload": raw_json,
        "kafka_topic": kafka_topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
        "schema_version": BRONZE_SCHEMA_VERSION,
        "processing_date": observed_ingestion_time.date(),
    }


def build_bronze_records(
    payloads: Iterable[dict[str, Any]],
    profile: SourceProfile,
    *,
    ingestion_time: datetime | None = None,
) -> list[dict[str, Any]]:
    return [
        build_bronze_record(payload, profile, ingestion_time=ingestion_time)
        for payload in payloads
    ]


def build_bronze_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("source_id", StringType(), False),
            StructField("source_type", StringType(), False),
            StructField("source_event_type", StringType(), True),
            StructField("event_time_raw", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("ingestion_time", TimestampType(), False),
            StructField("raw_payload", StringType(), True),
            StructField("kafka_topic", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("schema_version", StringType(), False),
            StructField("processing_date", DateType(), False),
        ]
    )


def _json_path(path: str | None) -> str | None:
    if not path:
        return None
    return "$." + ".".join(part for part in path.split(".") if part)


def _ensure_raw_input_columns(df: DataFrame) -> DataFrame:
    if RAW_INPUT_COLUMN not in df.columns and "value" in df.columns:
        df = df.withColumnRenamed("value", RAW_INPUT_COLUMN)
    if RAW_INPUT_COLUMN not in df.columns:
        raise ValueError("Bronze input DataFrame requires a raw_json or value column")

    if "kafka_topic" not in df.columns:
        df = df.withColumn("kafka_topic", F.lit(None).cast(StringType()))
    if "kafka_partition" not in df.columns:
        df = df.withColumn("kafka_partition", F.lit(None).cast(IntegerType()))
    if "kafka_offset" not in df.columns:
        df = df.withColumn("kafka_offset", F.lit(None).cast(LongType()))
    return df


def transform_bronze_dataframe(
    df: DataFrame,
    profile: SourceProfile,
) -> DataFrame:
    normalized = _ensure_raw_input_columns(df)
    raw_json_expr = F.col(RAW_INPUT_COLUMN)
    event_time_field = _json_path(profile.input.event_time_field)
    event_type_field = _json_path(profile.input.event_type_field)
    event_time_raw_expr = (
        F.get_json_object(raw_json_expr, event_time_field)
        if event_time_field is not None
        else F.lit(None).cast(StringType())
    )
    event_type_expr = (
        F.get_json_object(raw_json_expr, event_type_field)
        if event_type_field is not None
        else F.lit(None).cast(StringType())
    )
    event_basis = F.concat_ws(
        "|",
        F.lit(profile.source_id),
        F.coalesce(event_time_raw_expr.cast("string"), F.lit("")),
        F.coalesce(event_type_expr.cast("string"), F.lit("")),
        F.coalesce(raw_json_expr, F.lit("")),
        F.coalesce(F.col("kafka_topic").cast("string"), F.lit("")),
        F.coalesce(F.col("kafka_partition").cast("string"), F.lit("")),
        F.coalesce(F.col("kafka_offset").cast("string"), F.lit("")),
    )
    ingestion_time = F.current_timestamp()
    transformed = (
        normalized.withColumn("raw_payload", raw_json_expr)
        .withColumn("event_time_raw", event_time_raw_expr.cast("string"))
        .withColumn(
            "source_event_type",
            event_type_expr.cast("string"),
        )
        .withColumn("event_time", F.to_timestamp(F.col("event_time_raw")))
        .withColumn("ingestion_time", ingestion_time)
        .withColumn("event_id", F.sha2(event_basis, 256))
        .withColumn("source_id", F.lit(profile.source_id))
        .withColumn("source_type", F.lit(profile.source_type))
        .withColumn("schema_version", F.lit(BRONZE_SCHEMA_VERSION))
        .withColumn("processing_date", F.to_date(F.col("ingestion_time")))
    )
    return transformed.select(
        "event_id",
        "source_id",
        "source_type",
        "source_event_type",
        "event_time_raw",
        "event_time",
        "ingestion_time",
        "raw_payload",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "schema_version",
        "processing_date",
    )


def write_bronze_dataframe(
    df: DataFrame,
    output_path: str,
    partition_by: list[str],
) -> None:
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(output_path)
