from __future__ import annotations

from datetime import datetime, timezone

import pytest

from apps.spark.common import build_mapping_evaluator
from learnlake.contracts import MappingSpec, RouteSet
from learnlake.normalization import RouteMatcher, normalize_bronze_record
from learnlake.runtime import load_mapping_spec
from learnlake.runtime.config import resolve_path


def test_route_matcher_supports_leaf_and_composite_operators() -> None:
    route_set = RouteSet.model_validate(
        {
            "routes": [
                {
                    "id": "video-browser",
                    "priority": 100,
                    "match": {
                        "all": [
                            {"field": "raw_payload.event_source", "equals": "browser"},
                            {
                                "any": [
                                    {"field": "raw_payload.event_type", "in": ["play_video", "pause_video"]},
                                    {"field": "raw_payload.context.path", "contains": "/handler/play"},
                                ]
                            },
                            {
                                "not": {
                                    "field": "raw_payload.host",
                                    "equals": "studio.daotao.ai",
                                }
                            },
                        ]
                    },
                    "event_group": "video",
                    "normalized_type": "video.browser.interaction",
                    "action": "play",
                    "object_type": "video",
                    "learning_relevance": "learning",
                    "targets": ["silver_video_events"],
                },
                {
                    "id": "fallback",
                    "priority": 1,
                    "match": {"field": "raw_payload.event_type", "regex": ".+"},
                    "event_group": "unknown",
                    "normalized_type": "unknown",
                    "action": "unknown",
                    "object_type": "unknown",
                    "learning_relevance": "unknown",
                    "targets": ["silver_unknown_events"],
                },
            ]
        }
    )

    match = RouteMatcher(route_set).match(
        {
            "raw_payload": {
                "event_source": "browser",
                "event_type": "play_video",
                "host": "lms.daotao.ai",
                "context": {"path": "/courses/x/xblock/handler/play"},
            }
        }
    )

    assert match is not None
    assert match.id == "video-browser"


def test_route_matcher_uses_priority_then_route_id_for_tie_breaking() -> None:
    route_set = RouteSet.model_validate(
        {
            "routes": [
                {
                    "id": "b-route",
                    "priority": 100,
                    "match": {"field": "raw_payload.event_type", "equals": "play_video"},
                    "event_group": "video",
                    "normalized_type": "video.b",
                    "action": "b",
                    "object_type": "video",
                    "learning_relevance": "learning",
                    "targets": ["silver_video_events"],
                },
                {
                    "id": "a-route",
                    "priority": 100,
                    "match": {"field": "raw_payload.event_type", "equals": "play_video"},
                    "event_group": "video",
                    "normalized_type": "video.a",
                    "action": "a",
                    "object_type": "video",
                    "learning_relevance": "learning",
                    "targets": ["silver_video_events"],
                },
            ]
        }
    )

    match = RouteMatcher(route_set).match({"raw_payload": {"event_type": "play_video"}})

    assert match is not None
    assert match.id == "a-route"


def test_route_loading_rejects_unsupported_operator() -> None:
    with pytest.raises(ValueError):
        RouteSet.model_validate(
            {
                "routes": [
                    {
                        "id": "bad",
                        "match": {"field": "raw_payload.event_type", "ends_with": "video"},
                        "event_group": "unknown",
                        "normalized_type": "unknown",
                        "action": "unknown",
                        "object_type": "unknown",
                        "learning_relevance": "unknown",
                        "targets": ["silver_unknown_events"],
                    }
                ]
            }
        )


def test_route_loading_rejects_blank_extractor() -> None:
    with pytest.raises(ValueError):
        RouteSet.model_validate(
            {
                "routes": [
                    {
                        "id": "bad",
                        "match": {"field": "raw_payload.event_type", "equals": "x"},
                        "event_group": "unknown",
                        "normalized_type": "unknown",
                        "action": "unknown",
                        "object_type": "unknown",
                        "learning_relevance": "unknown",
                        "targets": ["silver_unknown_events"],
                        "extractor": "   ",
                    }
                ]
            }
        )


def test_unregistered_extractor_is_rejected_during_normalization() -> None:
    mapping = load_mapping_spec(resolve_path("catalog/mappings/daotao_ai_to_learning_event.yaml"))
    evaluator = build_mapping_evaluator(mapping, "daotao_ai")
    route_set = RouteSet.model_validate(
        {
            "routes": [
                {
                    "id": "missing-extractor",
                    "match": {"field": "raw_payload.event_type", "equals": "play_video"},
                    "event_group": "video",
                    "normalized_type": "video.browser.interaction",
                    "action": "play",
                    "object_type": "video",
                    "learning_relevance": "learning",
                    "targets": ["silver_video_events"],
                    "extractor": "missing_extractor",
                }
            ]
        }
    )
    bronze = {
        "event_id": "b1",
        "source_id": "daotao_ai",
        "source_type": "edx_tracking_log",
        "event_time": datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
        "ingestion_time": datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc),
        "raw_payload": {
            "time": "2026-01-01T10:00:00Z",
            "event_type": "play_video",
            "event_source": "browser",
            "username": "learner",
            "session": "s1",
            "agent": "Mozilla/5.0",
            "context": {
                "user_id": 1,
                "course_id": "course-v1:BK+TEST+2026",
                "org_id": "BK",
                "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@video+block@abc/handler/play",
            },
        },
    }

    with pytest.raises(KeyError):
        normalize_bronze_record(bronze, evaluator, route_set)

