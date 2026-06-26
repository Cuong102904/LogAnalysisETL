from __future__ import annotations

from datetime import date, datetime
import os

import pytest

from learnlake.runtime import build_spark
from projects.daotao_ai.gold.domain.behavior_anomalies.aggregator import (
    build_behavior_anomalies,
)
from projects.daotao_ai.gold.domain.exam_anomaly.aggregator import (
    build_exam_anomaly_features,
)
from projects.daotao_ai.gold.domain.learning_journey.aggregator import (
    build_learning_journey_features,
)
from projects.daotao_ai.gold.domain.pdf_behavior.aggregator import (
    build_pdf_behavior_features,
)
from projects.daotao_ai.gold.domain.quiz_performance.aggregator import (
    build_quiz_performance_features,
)
from projects.daotao_ai.gold.domain.video_anomaly.aggregator import (
    build_video_anomaly_features,
)
from projects.daotao_ai.gold.schemas.behavior_anomalies import (
    BEHAVIOR_ANOMALY_SIGNALS_SCHEMA,
)
from projects.daotao_ai.gold.schemas.exam_anomaly_features import (
    EXAM_INTEGRITY_SIGNALS_SCHEMA,
)
from projects.daotao_ai.gold.schemas.learning_journey_features import (
    USER_LEARNING_PROFILE_DAILY_SCHEMA,
)
from projects.daotao_ai.gold.schemas.pdf_behavior_features import (
    PDF_ENGAGEMENT_FEATURES_SCHEMA,
)
from projects.daotao_ai.gold.schemas.quiz_performance_features import (
    QUIZ_ATTEMPT_METRICS_SCHEMA,
)
from projects.daotao_ai.gold.schemas.video_anomaly_features import (
    VIDEO_FRICTION_SIGNALS_SCHEMA,
)


@pytest.fixture(scope="session")
def spark():
    previous_openlineage = os.environ.get("OPENLINEAGE_ENABLED")
    os.environ["OPENLINEAGE_ENABLED"] = "false"
    session = build_spark("test_daotao_gold_transforms")
    try:
        yield session
    finally:
        session.stop()
        if previous_openlineage is None:
            os.environ.pop("OPENLINEAGE_ENABLED", None)
        else:
            os.environ["OPENLINEAGE_ENABLED"] = previous_openlineage


def _schema_columns(schema) -> list[str]:
    return [field.name for field in schema]


def test_batch_gold_transforms_use_actual_silver_columns(spark) -> None:
    pdf_df = spark.createDataFrame(
        [
            {
                "course_id": "course-a",
                "actor_id": 101,
                "session_id": "pdf-s-1",
                "event_time": datetime(2026, 1, 6, 10, 0, 0),
                "document_action": "scroll",
                "document_type": "pdf",
                "document_id": "doc-1",
                "asset_url": "https://example.com/book.pdf",
                "file_name": "book.pdf",
                "chapter": "chapter-1",
                "chapter_title": "Chapter 1",
                "page_number": 3,
                "old_value": "old",
                "new_value": "new",
                "zoom_amount": None,
                "scroll_direction": "down",
                "search_query": "linear algebra",
                "search_status": "success",
                "case_sensitive": False,
                "highlight_all": True,
            },
            {
                "course_id": "course-a",
                "actor_id": 101,
                "session_id": "pdf-s-1",
                "event_time": datetime(2026, 1, 6, 10, 0, 30),
                "document_action": "zoom",
                "document_type": "pdf",
                "document_id": "doc-1",
                "asset_url": "https://example.com/book.pdf",
                "file_name": "book.pdf",
                "chapter": "chapter-1",
                "chapter_title": "Chapter 1",
                "page_number": 3,
                "old_value": None,
                "new_value": None,
                "zoom_amount": 1.5,
                "scroll_direction": None,
                "search_query": None,
                "search_status": None,
                "case_sensitive": None,
                "highlight_all": None,
            },
        ]
    )
    pdf_result = build_pdf_behavior_features(pdf_df)
    assert pdf_result.columns == _schema_columns(PDF_ENGAGEMENT_FEATURES_SCHEMA)
    pdf_row = pdf_result.collect()[0]
    assert pdf_row.event_count == 2
    assert pdf_row.scroll_count == 1
    assert pdf_row.zoom_count == 1
    assert pdf_row.scroll_balance == 1

    quiz_df = spark.createDataFrame(
        [
            {
                "course_id": "course-a",
                "actor_id": 201,
                "session_id": "quiz-s-1",
                "event_time": datetime(2026, 1, 6, 11, 0, 0),
                "problem_id": "prob-1",
                "assessment_action": "submit",
                "problem_display_name": "Problem 1",
                "response_type": "multiplechoice",
                "input_type": "checkbox",
                "attempts": 1,
                "success": "correct",
                "grade": 0.5,
                "max_grade": 1.0,
                "weighted_earned": 0.5,
                "weighted_possible": 1.0,
                "answers_json": {"answer": "A"},
                "correct_map_json": {"answer": "A"},
                "submission_json": {"submitted": True},
                "event_transaction_id": "txn-1",
            },
            {
                "course_id": "course-a",
                "actor_id": 201,
                "session_id": "quiz-s-1",
                "event_time": datetime(2026, 1, 6, 11, 0, 20),
                "problem_id": "prob-1",
                "assessment_action": "check",
                "problem_display_name": "Problem 1",
                "response_type": "multiplechoice",
                "input_type": "checkbox",
                "attempts": 2,
                "success": "incorrect",
                "grade": 0.0,
                "max_grade": 1.0,
                "weighted_earned": 0.0,
                "weighted_possible": 1.0,
                "answers_json": {"answer": "B"},
                "correct_map_json": {"answer": "A"},
                "submission_json": {"submitted": True},
                "event_transaction_id": "txn-2",
            },
        ]
    )
    quiz_result = build_quiz_performance_features(quiz_df)
    assert quiz_result.columns == _schema_columns(QUIZ_ATTEMPT_METRICS_SCHEMA)
    quiz_row = quiz_result.collect()[0]
    assert quiz_row.event_count == 2
    assert quiz_row.submit_count == 1
    assert quiz_row.check_count == 1
    assert quiz_row.attempt_count == 2

    learning_df = spark.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "source_id": "source-1",
                "source_type": "web",
                "raw_event_ref": "raw-1",
                "event_time": datetime(2026, 1, 6, 12, 0, 0),
                "ingest_time": datetime(2026, 1, 6, 12, 0, 1),
                "processing_time": datetime(2026, 1, 6, 12, 0, 2),
                "actor_id": 301,
                "actor_external_id": "ext-301",
                "session_id": "learn-s-1",
                "course_id": "course-a",
                "org_id": "org-a",
                "raw_name": "play_video",
                "raw_event_type": "play_video",
                "event_source": "browser",
                "event_group": "video",
                "normalized_type": "video.browser.interaction",
                "action": "interact",
                "object_type": "video",
                "object_id": "video-1",
                "learning_relevance": "learning",
                "path": "/courses/course-a",
                "page": "/courses/course-a/page",
                "referer": "https://example.com",
                "host": "lms.example.com",
                "ip": "127.0.0.1",
                "user_agent": "pytest",
                "is_authenticated": True,
                "is_bot": False,
                "is_noise": False,
                "quality_status": "valid",
                "quality_errors": ["none"],
                "payload_kind": "json",
                "payload_json": {"kind": "learning"},
                "context": {"section": "lesson"},
                "event_date": date(2026, 1, 6),
                "event_hour": 12,
            },
            {
                "event_id": "evt-2",
                "source_id": "source-2",
                "source_type": "web",
                "raw_event_ref": "raw-2",
                "event_time": datetime(2026, 1, 6, 12, 10, 0),
                "ingest_time": datetime(2026, 1, 6, 12, 10, 1),
                "processing_time": datetime(2026, 1, 6, 12, 10, 2),
                "actor_id": 301,
                "actor_external_id": None,
                "session_id": "learn-s-1",
                "course_id": "course-a",
                "org_id": "org-a",
                "raw_name": "textbook.pdf.page.scrolled",
                "raw_event_type": "textbook.pdf.page.scrolled",
                "event_source": "browser",
                "event_group": "document",
                "normalized_type": "document.browser.interaction",
                "action": "scroll",
                "object_type": "document",
                "object_id": "book.pdf",
                "learning_relevance": "learning",
                "path": None,
                "page": None,
                "referer": None,
                "host": None,
                "ip": None,
                "user_agent": None,
                "is_authenticated": True,
                "is_bot": False,
                "is_noise": False,
                "quality_status": "valid",
                "quality_errors": [],
                "payload_kind": "json",
                "payload_json": {},
                "context": {},
                "event_date": date(2026, 1, 6),
                "event_hour": 12,
            },
            {
                "event_id": "evt-3",
                "source_id": "source-3",
                "source_type": "web",
                "raw_event_ref": "raw-3",
                "event_time": datetime(2026, 1, 6, 12, 20, 0),
                "ingest_time": datetime(2026, 1, 6, 12, 20, 1),
                "processing_time": datetime(2026, 1, 6, 12, 20, 2),
                "actor_id": 301,
                "actor_external_id": None,
                "session_id": "learn-s-1",
                "course_id": "course-a",
                "org_id": "org-a",
                "raw_name": "edx.grades.problem.submitted",
                "raw_event_type": "edx.grades.problem.submitted",
                "event_source": "browser",
                "event_group": "assessment",
                "normalized_type": "assessment.answer.submit.browser",
                "action": "submit",
                "object_type": "problem",
                "object_id": "prob-1",
                "learning_relevance": "learning",
                "path": None,
                "page": None,
                "referer": None,
                "host": None,
                "ip": None,
                "user_agent": None,
                "is_authenticated": True,
                "is_bot": False,
                "is_noise": False,
                "quality_status": "valid",
                "quality_errors": [],
                "payload_kind": "json",
                "payload_json": {},
                "context": {},
                "event_date": date(2026, 1, 6),
                "event_hour": 12,
            },
            {
                "event_id": "evt-4",
                "source_id": "source-4",
                "source_type": "web",
                "raw_event_ref": "raw-4",
                "event_time": datetime(2026, 1, 6, 12, 30, 0),
                "ingest_time": datetime(2026, 1, 6, 12, 30, 1),
                "processing_time": datetime(2026, 1, 6, 12, 30, 2),
                "actor_id": 301,
                "actor_external_id": None,
                "session_id": "learn-s-1",
                "course_id": "course-a",
                "org_id": "org-a",
                "raw_name": "course.complete",
                "raw_event_type": "course.complete",
                "event_source": "browser",
                "event_group": "course_content",
                "normalized_type": "video.server.completion",
                "action": "complete",
                "object_type": "video",
                "object_id": "video-1",
                "learning_relevance": "learning",
                "path": None,
                "page": None,
                "referer": None,
                "host": None,
                "ip": None,
                "user_agent": None,
                "is_authenticated": True,
                "is_bot": False,
                "is_noise": False,
                "quality_status": "valid",
                "quality_errors": [],
                "payload_kind": "json",
                "payload_json": {},
                "context": {},
                "event_date": date(2026, 1, 6),
                "event_hour": 12,
            },
        ]
    )
    learning_result = build_learning_journey_features(learning_df)
    assert learning_result.columns == _schema_columns(USER_LEARNING_PROFILE_DAILY_SCHEMA)
    learning_row = learning_result.collect()[0]
    assert learning_row.event_count == 4
    assert learning_row.video_event_count == 1
    assert learning_row.pdf_event_count == 1
    assert learning_row.performance_event_count == 1
    assert learning_row.navigation_event_count == 1
    assert learning_row.completion_event_count == 1
    assert learning_row.completion_value_sum == 1.0


def test_streaming_gold_transforms_use_actual_silver_columns(spark) -> None:
    video_df = spark.createDataFrame(
        [
            {
                "course_id": "course-a",
                "actor_id": 401,
                "session_id": "video-s-1",
                "event_time": datetime(2026, 1, 6, 13, 0, 0),
                "video_id": "video-1",
                "video_action": "play",
                "video_code": "vid-001",
                "duration_seconds": 120.0,
                "current_time_seconds": 10.0,
                "old_time_seconds": 0.0,
                "new_time_seconds": 0.0,
                "old_speed": 1.0,
                "new_speed": 1.0,
                "saved_position": "00:10",
                "transcript_language": "en",
                "completion_status": "playing",
            },
            {
                "course_id": "course-a",
                "actor_id": 401,
                "session_id": "video-s-1",
                "event_time": datetime(2026, 1, 6, 13, 0, 10),
                "video_id": "video-1",
                "video_action": "pause",
                "video_code": "vid-001",
                "duration_seconds": 120.0,
                "current_time_seconds": 12.0,
                "old_time_seconds": 0.0,
                "new_time_seconds": 0.0,
                "old_speed": 1.0,
                "new_speed": 1.0,
                "saved_position": None,
                "transcript_language": "en",
                "completion_status": "paused",
            },
            {
                "course_id": "course-a",
                "actor_id": 402,
                "session_id": "video-s-2",
                "event_time": datetime(2026, 1, 6, 13, 0, 20),
                "video_id": "video-1",
                "video_action": "seek",
                "video_code": "vid-001",
                "duration_seconds": 120.0,
                "current_time_seconds": 20.0,
                "old_time_seconds": 8.0,
                "new_time_seconds": 20.0,
                "old_speed": 1.0,
                "new_speed": 1.0,
                "saved_position": None,
                "transcript_language": "en",
                "completion_status": "seeking",
            },
        ]
    )
    video_result = build_video_anomaly_features(video_df, bucket_seconds=5)
    assert video_result.columns == _schema_columns(VIDEO_FRICTION_SIGNALS_SCHEMA)
    assert {row.action_type for row in video_result.collect()} == {"play", "pause", "seek"}

    exam_df = spark.createDataFrame(
        [
            {
                "actor_id": 501,
                "session_id": "exam-s-1",
                "course_id": "course-a",
                "org_id": "org-a",
                "event_time": datetime(2026, 1, 6, 14, 0, 0),
                "exam_action": "lifecycle",
                "exam_id": 9001,
                "exam_content_id": "exam-1",
                "exam_name": "Final",
                "exam_default_time_limit_mins": 60,
                "exam_is_proctored": True,
                "exam_is_practice_exam": False,
                "exam_is_active": True,
                "attempt_id": 10001,
                "attempt_user_id": 501,
                "attempt_started_at": datetime(2026, 1, 6, 14, 0, 0),
                "attempt_completed_at": datetime(2026, 1, 6, 14, 45, 0),
                "attempt_status": "created",
                "attempt_elapsed_time_secs": 120,
                "quiz_nav_action": "ready_to_submit",
            },
            {
                "actor_id": 501,
                "session_id": "exam-s-1",
                "course_id": "course-a",
                "org_id": "org-a",
                "event_time": datetime(2026, 1, 6, 14, 20, 0),
                "exam_action": "proctoring",
                "exam_id": 9001,
                "exam_content_id": "exam-1",
                "exam_name": "Final",
                "exam_default_time_limit_mins": 60,
                "exam_is_proctored": True,
                "exam_is_practice_exam": False,
                "exam_is_active": True,
                "attempt_id": 10001,
                "attempt_user_id": 501,
                "attempt_started_at": datetime(2026, 1, 6, 14, 0, 0),
                "attempt_completed_at": datetime(2026, 1, 6, 14, 45, 0),
                "attempt_status": "submitted",
                "attempt_elapsed_time_secs": 1800,
                "quiz_nav_action": "ready_to_submit",
            },
            {
                "actor_id": 501,
                "session_id": "exam-s-1",
                "course_id": "course-a",
                "org_id": "org-a",
                "event_time": datetime(2026, 1, 6, 14, 40, 0),
                "exam_action": "lifecycle",
                "exam_id": 9001,
                "exam_content_id": "exam-1",
                "exam_name": "Final",
                "exam_default_time_limit_mins": 60,
                "exam_is_proctored": True,
                "exam_is_practice_exam": False,
                "exam_is_active": True,
                "attempt_id": 10001,
                "attempt_user_id": 501,
                "attempt_started_at": datetime(2026, 1, 6, 14, 0, 0),
                "attempt_completed_at": datetime(2026, 1, 6, 14, 45, 0),
                "attempt_status": "submitted",
                "attempt_elapsed_time_secs": 2000,
                "quiz_nav_action": "submit",
            },
        ]
    )
    system_df = spark.createDataFrame(
        [
            {
                "event_date": date(2026, 1, 6),
                "actor_id": 501,
                "session_id": "exam-s-1",
                "course_id": "course-a",
                "org_id": "org-a",
                "event_time": datetime(2026, 1, 6, 14, 25, 0),
                "system_action": "shell",
                "noise_type": "none",
                "path": "/login",
                "reason": "login",
            }
        ]
    )
    exam_result = build_exam_anomaly_features(exam_df, system_df)
    assert exam_result.columns == _schema_columns(EXAM_INTEGRITY_SIGNALS_SCHEMA)
    assert {row.anomaly_type for row in exam_result.collect()} == {
        "attempt_activity",
        "security_activity",
    }

    pdf_df = spark.createDataFrame(
        [
            {
                "course_id": "course-a",
                "actor_id": 601,
                "session_id": "pdf-s-1",
                "event_time": datetime(2026, 1, 6, 15, 0, 0),
                "document_action": "scroll",
                "document_type": "pdf",
                "document_id": "doc-1",
                "asset_url": "https://example.com/book.pdf",
                "file_name": "book.pdf",
                "chapter": "chapter-1",
                "chapter_title": "Chapter 1",
                "page_number": 1,
                "old_value": "old",
                "new_value": "new",
                "zoom_amount": 1.0,
                "scroll_direction": "down",
                "search_query": "linear algebra",
                "search_status": "success",
                "case_sensitive": False,
                "highlight_all": False,
            }
        ]
    )
    performance_df = spark.createDataFrame(
        [
            {
                "course_id": "course-a",
                "actor_id": 701,
                "session_id": "perf-s-1",
                "event_time": datetime(2026, 1, 6, 15, 10, 0),
                "problem_id": "prob-1",
                "assessment_action": "submit",
                "problem_display_name": "Problem 1",
                "response_type": "multiplechoice",
                "input_type": "checkbox",
                "attempts": 1,
                "success": "correct",
                "grade": 1.0,
                "max_grade": 1.0,
                "weighted_earned": 1.0,
                "weighted_possible": 1.0,
                "answers_json": {"answer": "A"},
                "correct_map_json": {"answer": "A"},
                "submission_json": {"submitted": True},
                "event_transaction_id": "txn-1",
            }
        ]
    )
    learning_df = spark.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "source_id": "source-1",
                "source_type": "web",
                "raw_event_ref": "raw-1",
                "event_time": datetime(2026, 1, 6, 15, 20, 0),
                "ingest_time": datetime(2026, 1, 6, 15, 20, 1),
                "processing_time": datetime(2026, 1, 6, 15, 20, 2),
                "actor_id": 801,
                "actor_external_id": "ext-801",
                "session_id": "learn-s-1",
                "course_id": "course-a",
                "org_id": "org-a",
                "raw_name": "play_video",
                "raw_event_type": "play_video",
                "event_source": "browser",
                "event_group": "video",
                "normalized_type": "video.browser.interaction",
                "action": "interact",
                "object_type": "video",
                "object_id": "video-1",
                "learning_relevance": "learning",
                "path": "/courses/course-a",
                "page": "/courses/course-a/page",
                "referer": "https://example.com",
                "host": "lms.example.com",
                "ip": "127.0.0.1",
                "user_agent": "pytest",
                "is_authenticated": True,
                "is_bot": False,
                "is_noise": False,
                "quality_status": "valid",
                "quality_errors": ["none"],
                "payload_kind": "json",
                "payload_json": {"kind": "learning"},
                "context": {"section": "practice"},
                "event_date": date(2026, 1, 6),
                "event_hour": 15,
            }
        ]
    )

    behavior_result = build_behavior_anomalies(video_df, pdf_df, performance_df, learning_df)
    assert behavior_result.columns == _schema_columns(BEHAVIOR_ANOMALY_SIGNALS_SCHEMA)
    assert {row.anomaly_domain for row in behavior_result.collect()} == {
        "video",
        "pdf",
        "performance",
        "journey",
    }
