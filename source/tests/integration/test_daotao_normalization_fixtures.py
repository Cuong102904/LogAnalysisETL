from __future__ import annotations

from datetime import datetime, timezone

from apps.spark.common import build_mapping_evaluator
from learnlake.ingestion import build_bronze_records
from learnlake.normalization import normalize_bronze_records
from learnlake.runtime import load_mapping_spec, load_quality_rules, load_route_set, load_source_profile
from learnlake.runtime.config import resolve_path


def _normalize_payloads(payloads: list[dict]) -> tuple[dict[str, list[dict]], list[dict]]:
    profile = load_source_profile("daotao_ai")
    bronze = build_bronze_records(
        payloads,
        profile,
        ingestion_time=datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc),
    )
    mapping = load_mapping_spec(resolve_path(profile.silver.mapping or ""))
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
    return batch.records_by_target, batch.invalid_records


def test_daotao_play_video_emits_event_index_and_video_fact() -> None:
    records_by_target, invalid_records = _normalize_payloads(
        [
            {
                "time": "2026-01-01T10:00:10Z",
                "event_type": "play_video",
                "event_source": "browser",
                "username": "learner_1",
                "session": "sess-1",
                "agent": "Mozilla/5.0",
                "event": {"id": "abc", "code": "vid-1", "duration": 90, "currentTime": 12},
                "context": {
                    "user_id": 101,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@video+block@abc/handler/play",
                },
            }
        ]
    )

    assert invalid_records == []
    assert len(records_by_target["silver_event_index"]) == 1
    assert len(records_by_target["silver_video_events"]) == 1
    event_index = records_by_target["silver_event_index"][0]
    fact = records_by_target["silver_video_events"][0]
    assert event_index["event_time"].isoformat() == "2026-01-01T10:00:10+00:00"
    assert event_index["action"] == "interact"
    assert fact["video_action"] == "play"
    assert fact["video_id"] == "abc"


def test_daotao_assessment_events_emit_typed_facts() -> None:
    records_by_target, invalid_records = _normalize_payloads(
        [
            {
                "time": "2026-01-01T10:00:00Z",
                "name": "problem_check",
                "event_type": "problem_check",
                "event_source": "browser",
                "username": "learner_2",
                "session": "sess-2",
                "agent": "Mozilla/5.0",
                "event": "input_1=choice_1",
                "context": {
                    "user_id": 102,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@problem+block@prob1/handler/xmodule_handler/problem_check",
                    "module": {"display_name": "Quiz 1"},
                },
            },
            {
                "time": "2026-01-01T10:00:03Z",
                "name": "problem_check",
                "event_type": "problem_check",
                "event_source": "server",
                "username": "learner_2",
                "session": "sess-2",
                "agent": "Mozilla/5.0",
                "event": {
                    "problem_id": "prob1",
                    "attempts": 2,
                    "success": "correct",
                    "grade": 1.0,
                    "max_grade": 1.0,
                    "answers": {"input_1": "choice_1"},
                    "correct_map": {"input_1": {"correctness": "correct"}},
                    "submission": {"response_type": "multiplechoiceresponse", "input_type": "choicegroup"},
                },
                "context": {
                    "user_id": 102,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@problem+block@prob1/handler/xmodule_handler/problem_check",
                    "module": {"display_name": "Quiz 1"},
                },
            },
            {
                "time": "2026-01-01T10:00:04Z",
                "name": "edx.grades.problem.submitted",
                "event_type": "edx.grades.problem.submitted",
                "event_source": "server",
                "username": "learner_2",
                "session": "sess-2",
                "agent": "Mozilla/5.0",
                "event": {
                    "problem_id": "prob1",
                    "weighted_earned": 1.0,
                    "weighted_possible": 1.0,
                    "event_transaction_id": "txn-1",
                },
                "context": {
                    "user_id": 102,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@problem+block@prob1/handler/xmodule_handler/problem_check",
                    "module": {"display_name": "Quiz 1"},
                },
            },
        ]
    )

    assert invalid_records == []
    assert len(records_by_target["silver_event_index"]) == 3
    assert len(records_by_target["silver_assessment_events"]) == 3
    check_fact = records_by_target["silver_assessment_events"][1]
    grade_fact = records_by_target["silver_assessment_events"][2]
    assert check_fact["assessment_action"] == "check"
    assert check_fact["problem_id"] == "prob1"
    assert check_fact["attempts"] == 2
    assert check_fact["response_type"] == "multiplechoiceresponse"
    assert grade_fact["assessment_action"] == "grade"
    assert grade_fact["weighted_earned"] == 1.0
    assert grade_fact["event_transaction_id"] == "txn-1"


def test_daotao_document_event_emits_scroll_fields() -> None:
    records_by_target, invalid_records = _normalize_payloads(
        [
            {
                "time": "2026-01-01T10:05:00Z",
                "name": "textbook.pdf.page.scrolled",
                "event_type": "textbook.pdf.page.scrolled",
                "event_source": "browser",
                "username": "learner_3",
                "session": "sess-3",
                "agent": "Mozilla/5.0",
                "event": {"direction": "down", "page": 17},
                "page": "https://lms.daotao.ai/static/pdfjs/web/viewer.html?file=https://cdn.example.com/CA.RISCV.2025-CH7.pdf",
                "context": {
                    "user_id": 103,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/pdfbook/0/",
                },
            }
        ]
    )

    assert invalid_records == []
    assert len(records_by_target["silver_document_events"]) == 1
    fact = records_by_target["silver_document_events"][0]
    assert fact["document_action"] == "scroll"
    assert fact["page_number"] == 17
    assert fact["scroll_direction"] == "down"
    assert fact["file_name"] == "CA.RISCV.2025-CH7.pdf"


def test_daotao_ambiguous_routes_choose_intended_domains() -> None:
    records_by_target, invalid_records = _normalize_payloads(
        [
            {
                "time": "2026-01-01T10:00:00Z",
                "name": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@problem+block@prob1/handler/xmodule_handler/problem_check",
                "event_type": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@problem+block@prob1/handler/xmodule_handler/problem_check",
                "event_source": "server",
                "username": "learner_2",
                "session": "sess-2",
                "agent": "Mozilla/5.0",
                "event": {"problem_id": "prob1"},
                "context": {"user_id": 102, "course_id": "course-v1:BK+TEST+2026", "org_id": "BK", "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@problem+block@prob1/handler/xmodule_handler/problem_check"},
            },
            {
                "time": "2026-01-01T10:01:00Z",
                "name": "/courses/course-v1:BK+TEST+2026/pdfbook/0/",
                "event_type": "/courses/course-v1:BK+TEST+2026/pdfbook/0/",
                "event_source": "server",
                "username": "learner_2",
                "session": "sess-2",
                "agent": "Mozilla/5.0",
                "context": {"user_id": 102, "course_id": "course-v1:BK+TEST+2026", "org_id": "BK", "path": "/courses/course-v1:BK+TEST+2026/pdfbook/0/"},
            },
            {
                "time": "2026-01-01T10:02:00Z",
                "name": "/api/edx_proctoring/v1/proctored_exam/attempt/42",
                "event_type": "/api/edx_proctoring/v1/proctored_exam/attempt/42",
                "event_source": "server",
                "username": "learner_2",
                "session": "sess-2",
                "agent": "Mozilla/5.0",
                "event": {"exam_id": 55},
                "context": {"user_id": 102, "course_id": "course-v1:BK+TEST+2026", "org_id": "BK", "path": "/api/edx_proctoring/v1/proctored_exam/attempt/42"},
            },
        ]
    )

    assert invalid_records == []
    event_index = records_by_target["silver_event_index"]
    assert [row["event_group"] for row in event_index] == ["assessment", "document", "exam"]


def test_invalid_required_event_index_fields_route_to_invalid_only() -> None:
    records_by_target, invalid_records = _normalize_payloads(
        [
            {
                "event_type": "play_video",
                "event_source": "browser",
                "username": "learner_1",
                "session": "sess-1",
                "agent": "Mozilla/5.0",
                "event": {"id": "abc"},
                "context": {
                    "user_id": 101,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@video+block@abc/handler/play",
                },
            }
        ]
    )

    assert records_by_target["silver_event_index"] == []
    assert "silver_video_events" not in records_by_target or records_by_target["silver_video_events"] == []
    assert len(invalid_records) == 1
    assert invalid_records[0]["quality_status"] == "invalid"


def test_source_event_time_remains_authoritative_over_ingest_order() -> None:
    records_by_target, invalid_records = _normalize_payloads(
        [
            {
                "time": "2026-01-01T10:10:00Z",
                "event_type": "play_video",
                "event_source": "browser",
                "username": "late-first",
                "session": "sess-a",
                "agent": "Mozilla/5.0",
                "event": {"id": "video-a"},
                "context": {
                    "user_id": 201,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@video+block@video-a/handler/play",
                },
            },
            {
                "time": "2026-01-01T10:01:00Z",
                "event_type": "play_video",
                "event_source": "browser",
                "username": "early-second",
                "session": "sess-b",
                "agent": "Mozilla/5.0",
                "event": {"id": "video-b"},
                "context": {
                    "user_id": 202,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@video+block@video-b/handler/play",
                },
            },
        ]
    )

    assert invalid_records == []
    event_times = [row["event_time"].isoformat() for row in records_by_target["silver_event_index"]]
    assert event_times == ["2026-01-01T10:10:00+00:00", "2026-01-01T10:01:00+00:00"]
