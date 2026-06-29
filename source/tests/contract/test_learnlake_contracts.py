from __future__ import annotations

from datetime import datetime, timezone

from learnlake.contracts import (
    EventIndex,
    MappingSpec,
    MetricDefinition,
    QualityRuleSet,
    SourceProfile,
)
from learnlake.ingestion.bronze_transform import build_bronze_schema
from learnlake.runtime import (
    load_mapping_spec,
    load_metric_definition,
    load_quality_rules,
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


def test_event_index_contract_accepts_required_fields() -> None:
    event = EventIndex(
        event_id="s1",
        source_id="source",
        source_type="format",
        raw_event_ref="b1",
        event_time=datetime.now(timezone.utc),
        processing_time=datetime.now(timezone.utc),
        raw_event_type="raw",
        event_group="navigation",
        normalized_type="navigation.page.view",
        action="view",
        object_type="page",
        learning_relevance="learning",
        is_authenticated=True,
        is_bot=False,
        payload_kind="json_dict",
    )

    assert event.quality_status == "valid"
    assert event.quality_errors == []


def test_catalog_contracts_load() -> None:
    profile = load_source_profile("daotao_ai")
    mapping = load_mapping_spec(resolve_path(profile.silver.mapping))
    rules = load_quality_rules([resolve_path(path) for path in profile.silver.quality_rules])
    metric = load_metric_definition(resolve_path(profile.metrics[0]))

    assert isinstance(profile, SourceProfile)
    assert isinstance(mapping, MappingSpec)
    assert isinstance(rules, QualityRuleSet)
    assert isinstance(metric, MetricDefinition)
    assert profile.source_id == "daotao_ai"
    assert profile.source_type == "edx_tracking_log"
