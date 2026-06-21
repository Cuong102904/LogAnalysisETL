from __future__ import annotations

from datetime import datetime, timezone

from learnlake.quality import validate_learning_event_record
from learnlake.runtime import load_quality_rules
from learnlake.runtime.config import resolve_path


def _rules():
    return load_quality_rules(
        [
            resolve_path("catalog/quality/common_learning_event_rules.yaml"),
            resolve_path("catalog/quality/daotao_ai_rules.yaml"),
        ]
    ).rules


def _record(**overrides):
    record = {
        "event_id": "s1",
        "source_id": "daotao_ai",
        "source_type": "edx_tracking_log",
        "event_time": datetime.now(timezone.utc),
        "raw_event_type": "play_video",
        "action": "play",
        "object_type": "video",
        "event_category": "learning",
        "learning_relevance": "learning",
        "is_authenticated": True,
        "is_bot": False,
        "quality_status": "valid",
        "quality_errors": [],
        "raw_event_ref": "b1",
        "processing_time": datetime.now(timezone.utc),
    }
    record.update(overrides)
    return record


def test_missing_event_time_is_invalid() -> None:
    result = validate_learning_event_record(_record(event_time=None), _rules())

    assert result.status == "invalid"
    assert "required_event_time" in result.errors


def test_bot_event_is_ignored() -> None:
    result = validate_learning_event_record(_record(is_bot=True), _rules())

    assert result.status == "ignored"
    assert "bot_or_crawler_event" in result.errors


def test_unknown_event_warns() -> None:
    result = validate_learning_event_record(_record(learning_relevance="unknown"), _rules())

    assert result.status == "warning"
    assert "unknown_learning_relevance" in result.errors
