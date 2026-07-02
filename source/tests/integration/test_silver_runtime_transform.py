from __future__ import annotations

import os

import pytest

from learnlake.ingestion import build_bronze_records
from learnlake.runtime import build_spark, load_source_profile
from learnlake.silver.runtime import load_silver_plan, transform_bronze_batch


@pytest.fixture(scope="session")
def spark():
    previous_openlineage = os.environ.get("OPENLINEAGE_ENABLED")
    os.environ["OPENLINEAGE_ENABLED"] = "false"
    session = build_spark("test_silver_runtime_transform")
    try:
        yield session
    finally:
        session.stop()
        if previous_openlineage is None:
            os.environ.pop("OPENLINEAGE_ENABLED", None)
        else:
            os.environ["OPENLINEAGE_ENABLED"] = previous_openlineage


def test_silver_runtime_static_batch_emits_canonical_domain_unknown_and_invalid(spark) -> None:
    profile = load_source_profile("daotao_ai")
    bronze_records = build_bronze_records(
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
                    "module": {"usage_key": "block-v1:BK+TEST+2026+type@video+block@abc", "display_name": "Lecture 1"},
                },
            },
            {
                "time": "2026-01-01T10:00:15Z",
                "event_type": "problem_check",
                "name": "problem_check",
                "event_source": "browser",
                "username": "learner_2",
                "session": "sess-2",
                "agent": "Mozilla/5.0",
                "event": "choice=1",
                "context": {
                    "user_id": 102,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@problem+block@prob1/handler/xmodule_handler/problem_check",
                    "module": {"usage_key": "block-v1:BK+TEST+2026+type@problem+block@prob1", "display_name": "Quiz 1"},
                },
            },
            {
                "time": "2026-01-01T10:00:20Z",
                "event_type": "edx.special_exam.timed.attempt.started",
                "event_source": "server",
                "username": "learner_2",
                "session": "sess-2",
                "agent": "Mozilla/5.0",
                "event": {
                    "attempt_id": 5001,
                    "exam_id": 91,
                    "exam_name": "Final Exam",
                    "exam_content_id": "exam-block",
                    "attempt_code": "A-5001",
                    "attempt_allowed_time_limit_mins": 90,
                    "attempt_status": "started",
                    "exam_is_proctored": True,
                    "exam_is_practice_exam": False,
                },
                "context": {
                    "user_id": 102,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@problem+block@prob1/handler/xmodule_handler/problem_check",
                },
            },
            {
                "time": "2026-01-01T10:00:25Z",
                "event_type": "mystery_event",
                "event_source": "browser",
                "username": "mystery",
                "session": "sess-x",
                "agent": "Mozilla/5.0",
                "event": {"foo": "bar"},
                "context": {
                    "user_id": 103,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/unknown",
                },
            },
            {
                "event_type": "play_video",
                "event_source": "browser",
                "username": "broken",
                "session": "sess-bad",
                "agent": "Mozilla/5.0",
                "event": {"id": "oops"},
                "context": {
                    "user_id": 104,
                    "course_id": "course-v1:BK+TEST+2026",
                    "org_id": "BK",
                    "path": "/courses/course-v1:BK+TEST+2026/xblock/block-v1:BK+TEST+2026+type@video+block@oops/handler/play",
                },
            },
        ],
        profile,
    )
    bronze_df = spark.createDataFrame(bronze_records)
    outputs = transform_bronze_batch(bronze_df, load_silver_plan("daotao_ai"))

    assert outputs["events_canonical"].count() == 3
    assert outputs["video_interactions"].count() == 1
    assert outputs["problem_submissions"].count() == 1
    assert outputs["exam_attempts"].count() == 1
    assert outputs["silver_unknown_events"].count() == 1
    assert outputs["silver_invalid_events"].count() == 1

    canonical_groups = {row.event_group for row in outputs["events_canonical"].select("event_group").collect()}
    assert canonical_groups >= {"video", "assessment", "exam"}
    exam_row = outputs["exam_attempts"].collect()[0]
    assert exam_row.exam_attempt_event_id is not None
    assert exam_row.exam_attempt_id == "5001"
    assert exam_row.attempt_status == "started"
    assert exam_row.attempt_event_type == "edx.special_exam.timed.attempt.started"
