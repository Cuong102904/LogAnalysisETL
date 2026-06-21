from __future__ import annotations

import pytest

from apps.spark.common import build_mapping_evaluator
from learnlake.contracts import MappingSpec
from learnlake.normalization import EventTypeResolver, MappingEvaluator
from learnlake.plugins import TransformRegistry
from learnlake.runtime import load_mapping_spec
from learnlake.runtime.config import resolve_path


def test_mapping_operations_and_event_type_resolver() -> None:
    mapping = load_mapping_spec(resolve_path("catalog/mappings/daotao_ai_to_learning_event.yaml"))
    evaluator = build_mapping_evaluator(mapping)
    record = {
        "bronze": {
            "event_id": "b1",
            "source_id": "daotao_ai",
            "source_type": "edx_tracking_log",
            "event_time": "2026-01-01T10:00:00Z",
        },
        "raw_payload": {
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
        "route": {
            "event_group": "video",
            "normalized_type": "video.browser.interaction",
            "action": "play",
            "object_type": "video",
            "learning_relevance": "learning",
            "is_noise": False,
        },
        "parsed_payload": {
            "kind": "json_dict",
            "data": {"id": "abc"},
        },
    }

    output = evaluator.evaluate_record(record)

    assert output["actor_id"] == "1"
    assert output["action"] == "play"
    assert output["object_type"] == "video"
    assert output["object_id"] == "abc"
    assert output["learning_relevance"] == "learning"
    assert output["is_authenticated"] is True
    assert output["is_bot"] is False


def test_unregistered_plugin_is_rejected() -> None:
    spec = MappingSpec.model_validate(
        {
            "source_id": "source",
            "target": "target",
            "fields": {
                "field": {
                    "plugin": {
                        "name": "missing_plugin",
                        "inputs": [],
                    }
                }
            },
        }
    )
    evaluator = MappingEvaluator(spec, registry=TransformRegistry())

    with pytest.raises(KeyError):
        evaluator.evaluate_record({"raw_payload": {}, "bronze": {}})


def test_event_type_resolver_unknown_fallback() -> None:
    resolver = EventTypeResolver.from_yaml(resolve_path("catalog/event_types/edx_tracking_log.yaml"))

    assert resolver.resolve("missing.event", "action") == "unknown"
    assert resolver.resolve("missing.event", "learning_relevance") == "unknown"


def test_event_type_resolver_uses_supplied_catalog_data() -> None:
    resolver = EventTypeResolver(
        {
            "mappings": {
                "custom.event": {
                    "action": "custom_action",
                    "object_type": "custom_object",
                    "event_category": "custom_category",
                    "learning_relevance": "learning",
                }
            }
        }
    )

    assert resolver.resolve("custom.event", "action") == "custom_action"
