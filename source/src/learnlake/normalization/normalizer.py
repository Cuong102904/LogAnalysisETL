from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from learnlake.contracts import (
    BatchNormalizationResult,
    EventIndex,
    FACT_MODEL_BY_TARGET,
    FactRecord,
    MappingSpec,
    NormalizationResult,
    RouteSet,
)
from learnlake.normalization.mapper import MappingEvaluator
from learnlake.normalization.payloads import parse_event_payload
from learnlake.normalization.router import RouteMatcher
from learnlake.quality.validator import ValidationResult, validate_record


def _record_for_mapping(
    bronze: dict[str, Any],
    *,
    route: dict[str, Any] | None = None,
    parsed_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "bronze": bronze,
        "raw_payload": bronze.get("raw_payload") or {},
        "route": route or {},
        "parsed_payload": parsed_payload or {},
    }


def normalize_bronze_record(
    bronze: dict[str, Any],
    evaluator: MappingEvaluator,
    route_set: RouteSet,
    *,
    validation_rules: list[Any] | None = None,
    processing_time: datetime | None = None,
) -> NormalizationResult:
    observed_processing_time = processing_time or datetime.now(timezone.utc)
    raw_payload = bronze.get("raw_payload") or {}
    parsed_payload = parse_event_payload(raw_payload.get("event"))
    route_matcher = RouteMatcher(route_set)
    route = route_matcher.match(
        {
            "bronze": bronze,
            "raw_payload": raw_payload,
            "parsed_payload": {
                "kind": parsed_payload.kind,
                "data": parsed_payload.data,
            },
        }
    )
    route_data = (
        {
            "id": route.id,
            "event_group": route.event_group,
            "normalized_type": route.normalized_type,
            "action": route.action,
            "object_type": route.object_type,
            "learning_relevance": route.learning_relevance,
            "is_noise": route.is_noise,
            "targets": route.targets,
            "extractor": route.extractor,
        }
        if route is not None
        else {
            "id": "unknown",
            "event_group": "unknown",
            "normalized_type": "unknown",
            "action": "unknown",
            "object_type": "unknown",
            "learning_relevance": "unknown",
            "is_noise": False,
            "targets": ["silver_unknown_events"],
            "extractor": None,
        }
    )
    mapped = evaluator.evaluate_record(
        _record_for_mapping(
            bronze,
            route=route_data,
            parsed_payload={"kind": parsed_payload.kind, "data": parsed_payload.data},
        )
    )
    if "processing_time" not in mapped or mapped["processing_time"] is None:
        mapped["processing_time"] = observed_processing_time
    if "payload_kind" not in mapped:
        mapped["payload_kind"] = parsed_payload.kind
    if "payload_json" not in mapped:
        mapped["payload_json"] = parsed_payload.json_dict()
    if "ingest_time" not in mapped:
        mapped["ingest_time"] = bronze.get("ingestion_time")
    if "event_date" not in mapped or mapped["event_date"] is None:
        event_time = mapped.get("event_time")
        if isinstance(event_time, datetime):
            mapped["event_date"] = event_time.date()
            mapped["event_hour"] = event_time.hour

    try:
        event = EventIndex.model_validate(mapped)
    except ValidationError as exc:
        return NormalizationResult(
            event_index=None,
            facts=[],
            invalid_records=[
                {
                    **mapped,
                    "quality_status": "invalid",
                    "quality_errors": [error["msg"] for error in exc.errors()],
                }
            ],
        )

    record = event.as_record()
    result = validate_record(record, validation_rules or [])
    record["quality_status"] = result.status
    record["quality_errors"] = result.errors

    if result.status in {"invalid", "ignored"}:
        return NormalizationResult(event_index=None, facts=[], invalid_records=[record])

    facts: list[FactRecord] = []
    extractor_name = route_data.get("extractor")
    if extractor_name:
        extractor = evaluator.registry.get(extractor_name)
        extracted = extractor(bronze, record, parsed_payload.data, route_data)
        for item in extracted or []:
            target_table = item["target_table"]
            model_cls = FACT_MODEL_BY_TARGET[target_table]
            fact = model_cls.model_validate(item["record"]).as_record()
            facts.append(FactRecord(target_table=target_table, record=fact))
    elif "silver_unknown_events" in route_data["targets"]:
        model_cls = FACT_MODEL_BY_TARGET["silver_unknown_events"]
        fact = model_cls.model_validate(
            {
                "event_id": record["event_id"],
                "event_time": record["event_time"],
                "actor_id": record.get("actor_id"),
                "session_id": record.get("session_id"),
                "course_id": record.get("course_id"),
                "org_id": record.get("org_id"),
                "unknown_action": "unknown",
                "raw_name": record.get("raw_name"),
                "raw_event_type": record.get("raw_event_type"),
            }
        ).as_record()
        facts.append(FactRecord(target_table="silver_unknown_events", record=fact))

    return NormalizationResult(event_index=record, facts=facts, invalid_records=[])


def normalize_bronze_records(
    bronze_records: Iterable[dict[str, Any]],
    mapping_spec: MappingSpec,
    evaluator: MappingEvaluator,
    route_set: RouteSet,
    *,
    validation_rules: list[Any] | None = None,
    processing_time: datetime | None = None,
) -> BatchNormalizationResult:
    valid_records: dict[str, list[dict[str, Any]]] = {"silver_event_index": []}
    invalid_records: list[dict[str, Any]] = []
    for bronze in bronze_records:
        result = normalize_bronze_record(
            bronze,
            evaluator,
            route_set,
            validation_rules=validation_rules,
            processing_time=processing_time,
        )
        if result.event_index is not None:
            valid_records.setdefault("silver_event_index", []).append(result.event_index)
        for fact in result.facts:
            valid_records.setdefault(fact.target_table, []).append(fact.record)
        invalid_records.extend(result.invalid_records)
    return BatchNormalizationResult(records_by_target=valid_records, invalid_records=invalid_records)
