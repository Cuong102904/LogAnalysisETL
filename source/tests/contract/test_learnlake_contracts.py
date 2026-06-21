from __future__ import annotations

from datetime import datetime, timezone

from learnlake.contracts import (
    BronzeEnvelope,
    EventIndex,
    MappingSpec,
    MetricDefinition,
    QualityRuleSet,
    SourceProfile,
)
from learnlake.runtime import (
    load_mapping_spec,
    load_metric_definition,
    load_quality_rules,
    load_source_profile,
)
from learnlake.runtime.config import resolve_path


def test_bronze_envelope_contract_accepts_required_fields() -> None:
    envelope = BronzeEnvelope(
        event_id="b1",
        source_id="source",
        source_type="format",
        ingestion_time=datetime.now(timezone.utc),
        raw_payload={"event_type": "x"},
        processing_date=datetime.now(timezone.utc).date(),
    )

    assert envelope.event_id == "b1"
    assert envelope.schema_version == "bronze-envelope-v1"


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
