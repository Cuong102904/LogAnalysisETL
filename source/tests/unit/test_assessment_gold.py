from __future__ import annotations

import os
from datetime import datetime

import pytest

from learnlake.runtime import build_spark
from projects.daotao_ai.gold.assessment_config import AssessmentBatchConfig, AssessmentStreamConfig
from projects.daotao_ai.gold.domain.assessment import (
    build_assessment_problem_daily_stats,
    build_exam_session_events,
    build_exam_session_snapshot,
    build_exam_windows,
    classify_problem_grades,
    classify_problem_submissions,
    resolve_course_mode,
)


@pytest.fixture(scope="session")
def spark():
    previous_openlineage = os.environ.get("OPENLINEAGE_ENABLED")
    os.environ["OPENLINEAGE_ENABLED"] = "false"
    session = build_spark("test_assessment_gold")
    try:
        yield session
    finally:
        session.stop()
        if previous_openlineage is None:
            os.environ.pop("OPENLINEAGE_ENABLED", None)
        else:
            os.environ["OPENLINEAGE_ENABLED"] = previous_openlineage


def test_assessment_configs_load_defaults() -> None:
    stream_config = AssessmentStreamConfig.from_env()
    batch_config = AssessmentBatchConfig.from_env()

    assert stream_config.app_name == "gold_assessment_stream"
    assert stream_config.output_exam_session_snapshot_path.endswith("/gold/assessment_exam_session_snapshot")
    assert stream_config.output_exam_session_events_path.endswith("/gold/assessment_exam_session_events")
    assert batch_config.app_name == "gold_assessment_batch"
    assert batch_config.output_problem_daily_stats_path.endswith("/gold/assessment_problem_daily_stats")


def test_context_helpers_classify_exam_and_practice_rows(spark) -> None:
    exam_attempts_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "course_id": "course-a",
                "exam_id": "exam-1",
                "exam_name": "Final exam",
                "attempt_id": "attempt-1",
                "user_id": "u-1",
                "attempt_event_type": "edx.special_exam.timed.attempt.started",
                "started_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "submitted_time_utc": None,
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 30, 0),
                "course_id": "course-a",
                "exam_id": "exam-1",
                "exam_name": "Final exam",
                "attempt_id": "attempt-1",
                "user_id": "u-1",
                "attempt_event_type": "edx.special_exam.timed.attempt.submitted",
                "started_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "submitted_time_utc": datetime(2026, 1, 18, 20, 30, 0),
            },
        ]
    )
    submissions_df = spark.createDataFrame(
        [
            {
                "submission_event_id": "sub-1",
                "event_time_utc": datetime(2026, 1, 18, 20, 5, 0),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "submission_source": "server",
                "attempt_no": 1,
                "is_correct": True,
                "grade_ratio": 1.0,
            },
            {
                "submission_event_id": "sub-2",
                "event_time_utc": datetime(2026, 1, 18, 21, 5, 0),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "submission_source": "server",
                "attempt_no": 2,
                "is_correct": False,
                "grade_ratio": 0.0,
            },
        ]
    )
    grades_df = spark.createDataFrame(
        [
            {
                "grade_event_id": "grade-1",
                "event_time_utc": datetime(2026, 1, 18, 20, 5, 5),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "is_correct": True,
                "grade_ratio": 1.0,
            },
            {
                "grade_event_id": "grade-2",
                "event_time_utc": datetime(2026, 1, 18, 21, 5, 5),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "is_correct": False,
                "grade_ratio": 0.0,
            },
        ]
    )

    exam_windows_df = build_exam_windows(exam_attempts_df)
    classified_submissions = classify_problem_submissions(submissions_df, exam_windows_df)
    classified_grades = classify_problem_grades(grades_df, exam_windows_df)
    course_mode_rows = resolve_course_mode(exam_windows_df, classified_submissions).collect()

    assert classified_submissions.filter("context = 'exam'").count() == 1
    assert classified_submissions.filter("context = 'practice'").count() == 1
    assert classified_grades.filter("context = 'exam'").count() == 1
    assert classified_grades.filter("context = 'practice'").count() == 1
    assert course_mode_rows[0].course_mode == "mixed"


def test_exam_session_snapshot_and_events_use_shared_state(spark) -> None:
    exam_attempts_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "course_id": "course-a",
                "exam_id": "exam-1",
                "exam_name": "Final exam",
                "attempt_id": "attempt-1",
                "user_id": "u-1",
                "attempt_event_type": "edx.special_exam.timed.attempt.started",
                "attempt_status": "started",
                "started_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "submitted_time_utc": None,
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 30, 0),
                "course_id": "course-a",
                "exam_id": "exam-1",
                "exam_name": "Final exam",
                "attempt_id": "attempt-1",
                "user_id": "u-1",
                "attempt_event_type": "edx.special_exam.timed.attempt.submitted",
                "attempt_status": "submitted",
                "started_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "submitted_time_utc": datetime(2026, 1, 18, 20, 30, 0),
            },
        ]
    )
    submissions_df = spark.createDataFrame(
        [
            {
                "submission_event_id": "sub-1",
                "event_time_utc": datetime(2026, 1, 18, 20, 5, 0),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "submission_source": "server",
                "attempt_no": 1,
            },
            {
                "submission_event_id": "sub-2",
                "event_time_utc": datetime(2026, 1, 18, 20, 7, 0),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "submission_source": "server",
                "attempt_no": 2,
            },
        ]
    )

    snapshot_rows = build_exam_session_snapshot(exam_attempts_df, submissions_df).collect()
    events_rows = build_exam_session_events(exam_attempts_df, submissions_df).collect()

    assert len(snapshot_rows) == 1
    snapshot = snapshot_rows[0]
    assert snapshot.submission_count == 2
    assert snapshot.retry_count == 1
    assert snapshot.last_problem_id == "problem-1"
    assert snapshot.current_status == "submitted"
    assert snapshot.is_active is False

    assert {row.event_type for row in events_rows} == {
        "edx.special_exam.timed.attempt.started",
        "edx.special_exam.timed.attempt.submitted",
        "problem_submission",
    }
    assert sum(1 for row in events_rows if row.event_type == "problem_submission") == 2


def test_daily_problem_stats_aggregate_context_and_course_mode(spark) -> None:
    exam_attempts_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "course_id": "course-a",
                "exam_id": "exam-1",
                "exam_name": "Final exam",
                "attempt_id": "attempt-1",
                "user_id": "u-1",
                "attempt_event_type": "edx.special_exam.timed.attempt.started",
                "started_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "submitted_time_utc": datetime(2026, 1, 18, 20, 30, 0),
            }
        ]
    )
    submissions_df = spark.createDataFrame(
        [
            {
                "submission_event_id": "sub-1",
                "event_time_utc": datetime(2026, 1, 18, 20, 5, 0),
                "event_date": datetime(2026, 1, 18, 20, 5, 0).date(),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "submission_source": "server",
                "attempt_no": 1,
            },
            {
                "submission_event_id": "sub-2",
                "event_time_utc": datetime(2026, 1, 18, 21, 5, 0),
                "event_date": datetime(2026, 1, 18, 21, 5, 0).date(),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "submission_source": "server",
                "attempt_no": 2,
            },
        ]
    )
    grades_df = spark.createDataFrame(
        [
            {
                "grade_event_id": "grade-1",
                "event_time_utc": datetime(2026, 1, 18, 20, 5, 5),
                "event_date": datetime(2026, 1, 18, 20, 5, 5).date(),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "is_correct": True,
                "grade_ratio": 1.0,
            },
            {
                "grade_event_id": "grade-2",
                "event_time_utc": datetime(2026, 1, 18, 21, 5, 5),
                "event_date": datetime(2026, 1, 18, 21, 5, 5).date(),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "is_correct": False,
                "grade_ratio": 0.0,
            },
        ]
    )

    stats_rows = build_assessment_problem_daily_stats(
        build_exam_windows(exam_attempts_df),
        submissions_df,
        grades_df,
        course_mode_submissions_df=submissions_df,
    ).collect()

    stats_by_context = {row.context: row for row in stats_rows}
    assert stats_by_context["exam"].submission_count == 1
    assert stats_by_context["exam"].retry_count == 0
    assert stats_by_context["exam"].correct_count == 1
    assert stats_by_context["practice"].submission_count == 1
    assert stats_by_context["practice"].retry_count == 1
    assert stats_by_context["practice"].wrong_count == 1
    assert all(row.course_mode == "mixed" for row in stats_rows)
