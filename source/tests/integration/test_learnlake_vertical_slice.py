from __future__ import annotations

import inspect
from datetime import datetime, timezone

from apps.spark.common import build_mapping_evaluator
from learnlake.connectors import read_json_lines
from learnlake.ingestion import build_bronze_records
from learnlake.metrics import build_course_activity_summary, course_activity
from learnlake.normalization import normalize_bronze_records
from learnlake.runtime import load_mapping_spec, load_quality_rules, load_route_set, load_source_profile
from learnlake.runtime.config import resolve_path


def test_daotao_fixture_bronze_silver_gold_vertical_slice() -> None:
    profile = load_source_profile("daotao_ai")
    payloads = list(read_json_lines(resolve_path(profile.input.path or "")))
    bronze = build_bronze_records(
        payloads,
        profile,
        ingestion_time=datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc),
    )

    assert {record["source_id"] for record in bronze} == {"daotao_ai"}
    assert all(record["raw_payload"] for record in bronze)
    assert [record["event_time"].isoformat() for record in bronze[:2]] == [
        "2026-01-01T10:00:30+00:00",
        "2026-01-01T10:00:10+00:00",
    ]

    mapping = load_mapping_spec(resolve_path(profile.silver.mapping))
    routes = load_route_set(resolve_path(profile.silver.routing))
    evaluator = build_mapping_evaluator(mapping, profile.source_id)
    rules = load_quality_rules([resolve_path(path) for path in profile.silver.quality_rules]).rules
    batch = normalize_bronze_records(
        bronze,
        mapping,
        evaluator,
        routes,
        validation_rules=rules,
        processing_time=datetime(2026, 1, 1, 11, 5, tzinfo=timezone.utc),
    )
    silver = batch.records_by_target["silver_event_index"]
    invalid = batch.invalid_records

    assert len(silver) == 4
    assert len(invalid) == 1
    assert all(record["event_id"] for record in silver)
    assert all(record["raw_event_ref"] for record in silver)
    assert {record["learning_relevance"] for record in silver} >= {"learning", "unknown"}
    assert invalid[0]["quality_status"] == "ignored"

    assert len(batch.records_by_target["silver_video_events"]) == 1
    assert len(batch.records_by_target["silver_assessment_events"]) == 1
    assert len(batch.records_by_target["silver_navigation_events"]) == 1
    assert len(batch.records_by_target["silver_unknown_events"]) == 1

    gold = build_course_activity_summary(silver + invalid)

    assert len(gold) == 1
    summary = gold[0]
    assert summary["source_id"] == "daotao_ai"
    assert summary["active_learners"] == 3
    assert summary["learning_event_count"] == 3
    assert summary["noise_event_count"] == 1
    assert summary["unknown_event_count"] == 1
    assert summary["total_event_count"] == 5


def test_course_activity_metric_does_not_parse_raw_payload() -> None:
    source = inspect.getsource(course_activity.build_course_activity_summary)

    assert "raw_payload" not in source
