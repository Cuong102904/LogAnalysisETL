from __future__ import annotations

from learnlake.ingestion.bronze_transform import build_bronze_schema
from learnlake.runtime import (
    load_quality_rules,
    load_silver_parser_config,
    load_silver_routing_config,
    load_source_profile,
)
from learnlake.runtime.config import resolve_path
from pyspark.sql.types import StringType


def test_bronze_schema_uses_raw_string_payload() -> None:
    schema = build_bronze_schema()

    assert [field.name for field in schema.fields] == [
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
    ]
    assert schema["raw_payload"].dataType == StringType()


def test_daotao_silver_catalog_contracts_load() -> None:
    profile = load_source_profile("daotao_ai")
    routing = load_silver_routing_config(resolve_path(profile.silver.routing))
    parsers = load_silver_parser_config(resolve_path(profile.silver.parsers))
    rules = load_quality_rules([resolve_path(path) for path in profile.silver.quality_rules])

    assert profile.source_id == "daotao_ai"
    assert profile.silver.canonical.table == "events_canonical"
    assert profile.silver.runtime.trigger_processing_time == "10 seconds"
    assert profile.silver.runtime.max_files_per_trigger == 100
    assert routing.version == "2026-07-02"
    assert len(routing.routes) >= 8
    assert parsers.version == "2026-07-02"
    assert any(parser.name == "parse_problem_check_browser" for parser in parsers.parsers)
    assert len(rules.rules) >= 2
