from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timezone
from functools import lru_cache
from types import NoneType, UnionType
from typing import Any, Union, get_args, get_origin

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from apps.spark.common import (
    load_source_mapping,
    load_source_quality_rules,
    read_records,
    write_records,
)
from learnlake.contracts import EventIndex, FACT_MODEL_BY_TARGET
from learnlake.normalization import normalize_bronze_records
from learnlake.runtime import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LearnLake Silver normalization.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--invalid-output")
    parser.add_argument("--stream", action="store_true", help="Run Delta-to-Delta streaming mode.")
    return parser.parse_args()


def _decode_bronze_record(record: dict[str, Any]) -> dict[str, Any]:
    decoded = dict(record)
    raw_payload = decoded.get("raw_payload")
    if isinstance(raw_payload, str):
        decoded["raw_payload"] = json.loads(raw_payload)
    return decoded


def _spark_silver_record(record: dict[str, Any]) -> dict[str, Any]:
    converted = dict(record)
    for key, value in list(converted.items()):
        if isinstance(value, (dict, list)):
            converted[key] = json.dumps(value, sort_keys=True, default=str)
    return converted


def _write_delta_batch(
    spark: SparkSession,
    table_name: str,
    records: list[dict[str, Any]],
    output_path: str | None,
) -> None:
    if not records or not output_path:
        return
    df = spark.createDataFrame(
        [_spark_silver_record(record) for record in records],
        _schema_for_table(table_name),
    )
    df.write.format("delta").mode("append").save(output_path)


def _write_delta_targets(
    spark: SparkSession,
    records_by_target: dict[str, list[dict[str, Any]]],
    paths_by_target: dict[str, str],
) -> None:
    for table_name, records in records_by_target.items():
        _write_delta_batch(spark, table_name, records, paths_by_target.get(table_name))


def _spark_type_for_annotation(annotation: Any) -> tuple[Any, bool]:
    origin = get_origin(annotation)
    if origin in {UnionType, Union}:
        args = [arg for arg in get_args(annotation) if arg is not NoneType]
        nullable = len(args) != len(get_args(annotation))
        if len(args) == 1:
            spark_type, _ = _spark_type_for_annotation(args[0])
            return spark_type, nullable
        return StringType(), True
    if annotation in {str, Any}:
        return StringType(), False
    if annotation is int:
        return LongType(), False
    if annotation is float:
        return DoubleType(), False
    if annotation is bool:
        return BooleanType(), False
    if annotation is datetime:
        return TimestampType(), False
    if annotation is date:
        return DateType(), False
    if origin in {dict, list}:
        return StringType(), False
    return StringType(), False


@lru_cache(maxsize=None)
def _schema_for_table(table_name: str) -> StructType:
    model_cls = EventIndex if table_name == "silver_event_index" else FACT_MODEL_BY_TARGET.get(table_name)
    if model_cls is None:
        model_cls = EventIndex
    fields = []
    for field_name, field_info in model_cls.model_fields.items():
        spark_type, nullable = _spark_type_for_annotation(field_info.annotation)
        fields.append(StructField(field_name, spark_type, nullable or not field_info.is_required()))
    return StructType(fields)


def _silver_output_paths(profile: Any, *, output_override: str | None = None) -> dict[str, str]:
    outputs = {profile.silver.event_index.table: output_override or profile.silver.event_index.path}
    for target in profile.silver.targets.values():
        outputs[target.table] = target.path
    return outputs


def _wait_for_delta_table(spark: SparkSession, path: str, timeout_seconds: int = 180) -> None:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    delta_log_path = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_delta_log")
    first_commit_path = jvm.org.apache.hadoop.fs.Path(
        path.rstrip("/") + "/_delta_log/00000000000000000000.json"
    )
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        fs = delta_log_path.getFileSystem(hadoop_conf)
        if fs.exists(first_commit_path):
            return
        time.sleep(2)
    raise TimeoutError(f"Delta table schema was not created before timeout: {path}")


def run_stream() -> int:
    args = parse_args()
    profile, mapping, routes, evaluator = load_source_mapping(args.source)
    rules = load_source_quality_rules(args.source)
    input_path = args.input or profile.bronze.path
    output_paths = _silver_output_paths(profile, output_override=args.output)
    invalid_path = args.invalid_output or profile.silver.invalid_path
    if not profile.silver.event_index.checkpoint:
        raise ValueError("Silver checkpoint path is required for streaming")

    spark = build_spark("learnlake_silver_normalization")
    _wait_for_delta_table(spark, input_path)
    bronze_stream = spark.readStream.format("delta").load(input_path)

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        bronze_records = [_decode_bronze_record(row.asDict(recursive=True)) for row in batch_df.collect()]
        batch_result = normalize_bronze_records(
            bronze_records,
            mapping,
            evaluator,
            routes,
            validation_rules=rules,
            processing_time=datetime.now(timezone.utc),
        )
        _write_delta_targets(spark, batch_result.records_by_target, output_paths)
        _write_delta_batch(
            spark,
            profile.silver.invalid.table if profile.silver.invalid is not None else "silver_invalid_events",
            batch_result.invalid_records,
            invalid_path,
        )
        total_valid = sum(len(records) for records in batch_result.records_by_target.values())
        total = total_valid + len(batch_result.invalid_records)
        if total:
            print(
                f"learnlake silver batch_id={batch_id} wrote valid={total_valid} invalid={len(batch_result.invalid_records)}",
                flush=True,
            )

    (
        bronze_stream.writeStream.option("checkpointLocation", profile.silver.event_index.checkpoint)
        .foreachBatch(process_batch)
        .queryName("learnlake_silver_normalization")
        .start()
        .awaitTermination()
    )
    return 0


def main() -> int:
    args = parse_args()
    if args.stream:
        return run_stream()
    profile, mapping, routes, evaluator = load_source_mapping(args.source)
    rules = load_source_quality_rules(args.source)
    input_path = args.input or profile.bronze.path
    output_paths = _silver_output_paths(profile, output_override=args.output)
    invalid_path = args.invalid_output or profile.silver.invalid_path
    bronze_records = read_records(input_path)
    batch_result = normalize_bronze_records(
        bronze_records,
        mapping,
        evaluator,
        routes,
        validation_rules=rules,
        processing_time=datetime.now(timezone.utc),
    )
    for table_name, records in batch_result.records_by_target.items():
        output_path = output_paths.get(table_name)
        if output_path:
            write_records(output_path, records)
    if invalid_path:
        write_records(invalid_path, batch_result.invalid_records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
