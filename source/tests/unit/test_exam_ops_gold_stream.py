from __future__ import annotations

import os
from datetime import datetime

import pytest

from learnlake.runtime import build_spark
from projects.daotao_ai.gold.domain.exam_ops import (
    build_exam_ops_base,
    build_exam_windows,
    build_gold_exam_attempt_timeline,
    build_gold_exam_attempt_flow_10s,
    build_gold_exam_load_10s,
    build_gold_exam_question_metrics,
)
from projects.daotao_ai.gold.exam_ops_config import GoldExamOpsConfig


@pytest.fixture(scope="session")
def spark():
    previous_openlineage = os.environ.get("OPENLINEAGE_ENABLED")
    os.environ["OPENLINEAGE_ENABLED"] = "false"
    session = build_spark("test_exam_ops_gold_stream")
    try:
        yield session
    finally:
        session.stop()
        if previous_openlineage is None:
            os.environ.pop("OPENLINEAGE_ENABLED", None)
        else:
            os.environ["OPENLINEAGE_ENABLED"] = previous_openlineage


def test_exam_ops_config_loads_defaults_from_catalog() -> None:
    config = GoldExamOpsConfig.from_env()

    assert config.app_name == "gold_exam_ops_stream"
    assert config.input_exam_attempts_path.endswith("/silver/exam_attempts")
    assert config.input_events_canonical_path.endswith("/silver/events_canonical")
    assert config.input_problem_submissions_path.endswith("/silver/problem_submissions")
    assert config.input_problem_grades_path.endswith("/silver/problem_grades")
    assert config.output_exam_load_10s_path.endswith("/gold/gold_exam_load_10s")
    assert config.output_exam_attempt_flow_10s_path.endswith("/gold/gold_exam_attempt_flow_10s")
    assert config.output_exam_attempt_timeline_path.endswith("/gold/gold_exam_attempt_timeline")
    assert config.output_exam_question_metrics_path.endswith("/gold/gold_exam_question_metrics")
    assert config.trigger_interval_seconds == 5
    assert config.max_files_per_trigger == 2


def test_exam_ops_base_and_aggregates_use_exam_attempt_fields(spark) -> None:
    exam_attempts_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 23, 41),
                "course_id": "course-a",
                "exam_id": "2094",
                "exam_name": "Mock exam",
                "attempt_id": "356567",
                "exam_attempt_id": "legacy-356567",
                "user_id": "12272",
                "attempt_event_type": "edx.special_exam.timed.attempt.created",
                "attempt_status": "created",
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 23, 45),
                "course_id": "course-a",
                "exam_id": "2094",
                "exam_name": "Mock exam",
                "attempt_id": "356567",
                "exam_attempt_id": "legacy-356567",
                "user_id": "12272",
                "attempt_event_type": "edx.special_exam.timed.attempt.started",
                "attempt_status": "started",
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 23, 49),
                "course_id": "course-a",
                "exam_id": "2094",
                "exam_name": "Mock exam",
                "attempt_id": "356567",
                "exam_attempt_id": "legacy-356567",
                "user_id": "12272",
                "attempt_event_type": "edx.special_exam.timed.attempt.ready_to_submit",
                "attempt_status": "ready_to_submit",
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 23, 52),
                "course_id": "course-a",
                "exam_id": "2094",
                "exam_name": "Mock exam",
                "attempt_id": "356567",
                "exam_attempt_id": "legacy-356567",
                "user_id": "12272",
                "attempt_event_type": "edx.special_exam.timed.attempt.submitted",
                "attempt_status": "submitted",
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 23, 54),
                "course_id": "course-a",
                "exam_id": "2094",
                "exam_name": "Mock exam",
                "attempt_id": "356568",
                "exam_attempt_id": "legacy-356568",
                "user_id": "12273",
                "attempt_event_type": "edx.special_exam.timed.attempt.started",
                "attempt_status": "started",
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 23, 56),
                "course_id": "course-a",
                "exam_id": "2094",
                "exam_name": "Mock exam",
                "attempt_id": "356568",
                "exam_attempt_id": "legacy-356568",
                "user_id": "12273",
                "attempt_event_type": "ignore.me",
                "attempt_status": "ignored",
            },
        ]
    )

    base_df = build_exam_ops_base(exam_attempts_df)
    base_rows = base_df.collect()

    assert len(base_rows) == 5
    assert {row.bucket_10s.second for row in base_rows} == {40, 50}

    load_rows = {
        row.bucket_10s.second: row for row in build_gold_exam_load_10s(base_df).collect()
    }
    assert load_rows[40].created_count == 1
    assert load_rows[40].started_count == 1
    assert load_rows[40].ready_to_submit_count == 1
    assert load_rows[40].submitted_count == 0
    assert load_rows[40].distinct_users == 1
    assert load_rows[40].distinct_attempts == 1
    assert load_rows[40].total_events == 3

    assert load_rows[50].created_count == 0
    assert load_rows[50].started_count == 1
    assert load_rows[50].ready_to_submit_count == 0
    assert load_rows[50].submitted_count == 1
    assert load_rows[50].distinct_users == 2
    assert load_rows[50].distinct_attempts == 2
    assert load_rows[50].total_events == 2

    flow_rows = {
        (row.bucket_10s.second, row.flow_stage): row
        for row in build_gold_exam_attempt_flow_10s(base_df).collect()
    }
    assert flow_rows[(40, "created")].event_count == 1
    assert flow_rows[(40, "started")].event_count == 1
    assert flow_rows[(40, "ready_to_submit")].event_count == 1
    assert flow_rows[(50, "submitted")].event_count == 1
    assert flow_rows[(50, "started")].distinct_users == 1
    assert flow_rows[(50, "started")].distinct_attempts == 1


def test_exam_ops_base_falls_back_to_exam_attempt_id_when_attempt_id_missing(spark) -> None:
    exam_attempts_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 23, 41),
                "course_id": "course-a",
                "exam_id": "2094",
                "exam_name": "Mock exam",
                "exam_attempt_id": "356567",
                "user_id": "12272",
                "attempt_event_type": "edx.special_exam.timed.attempt.created",
                "attempt_status": "created",
            }
        ]
    )

    row = build_exam_ops_base(exam_attempts_df).collect()[0]

    assert row.attempt_id == "356567"


def test_build_exam_windows_timeline_and_question_metrics(spark) -> None:
    exam_attempts_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "course_id": "course-a",
                "exam_id": "exam-1",
                "exam_name": "Final exam",
                "attempt_id": "attempt-1",
                "user_id": "u-1",
                "session_id": "session-1",
                "attempt_event_type": "edx.special_exam.timed.attempt.started",
                "started_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "submitted_time_utc": None,
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 10, 0),
                "course_id": "course-a",
                "exam_id": "exam-1",
                "exam_name": "Final exam",
                "attempt_id": "attempt-1",
                "user_id": "u-1",
                "session_id": "session-1",
                "attempt_event_type": "edx.special_exam.timed.attempt.submitted",
                "started_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "submitted_time_utc": datetime(2026, 1, 18, 20, 10, 0),
            },
        ]
    )
    exam_windows_df = build_exam_windows(exam_attempts_df)
    exam_window = exam_windows_df.collect()[0]

    assert exam_window.exam_attempt_id == "attempt-1"
    assert exam_window.window_start_utc == datetime(2026, 1, 18, 20, 0, 0)
    assert exam_window.window_end_utc == datetime(2026, 1, 18, 20, 10, 0)
    assert exam_window.is_submitted is True

    browser_events_df = spark.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "event_time_utc": datetime(2026, 1, 18, 20, 1, 0),
                "course_id": "course-a",
                "user_id": "u-1",
                "event_source": "browser",
                "event_group": "assessment",
                "event_subgroup": "problem_submission",
                "event_type": "problem_check",
                "event_name": "problem_check",
                "module_usage_key": "problem-1",
                "module_display_name": "Question 1",
            },
            {
                "event_id": "evt-2",
                "event_time_utc": datetime(2026, 1, 18, 20, 2, 0),
                "course_id": "course-a",
                "user_id": "u-1",
                "event_source": "browser",
                "event_group": "navigation",
                "event_subgroup": "flow",
                "event_type": "edx.ui.lms.sequence.next_selected",
                "event_name": "edx.ui.lms.sequence.next_selected",
                "module_usage_key": None,
                "module_display_name": None,
            },
        ]
    )
    browser_submissions_df = spark.createDataFrame(
        [
            {
                "submission_event_id": "evt-1",
                "module_display_name": "Question 1",
                "answer_payload": "choice_a",
                "success": "correct",
                "grade_raw": 1.0,
                "max_grade_raw": 1.0,
            }
        ]
    )
    timeline_rows = build_gold_exam_attempt_timeline(
        exam_windows_df,
        browser_events_df,
        browser_submissions_df,
    ).orderBy("event_sequence_no").collect()

    assert len(timeline_rows) == 2
    assert timeline_rows[0].canonical_event_id == "evt-1"
    assert timeline_rows[0].answer_payload == "choice_a"
    assert timeline_rows[0].event_sequence_no == 1
    assert timeline_rows[1].canonical_event_id == "evt-2"
    assert timeline_rows[1].event_sequence_no == 2

    server_submissions_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 1, 0),
                "course_id": "course-a",
                "user_id": "u-1",
                "submission_source": "server",
                "problem_id": "problem-1",
                "module_usage_key": "problem-1",
                "module_display_name": "Question 1",
                "submission_event_id": "srv-1",
                "attempt_no": 1,
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 3, 0),
                "course_id": "course-a",
                "user_id": "u-1",
                "submission_source": "server",
                "problem_id": "problem-1",
                "module_usage_key": "problem-1",
                "module_display_name": "Question 1",
                "submission_event_id": "srv-2",
                "attempt_no": 2,
            },
        ]
    )
    grades_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 1, 10),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "module_usage_key": "problem-1",
                "module_display_name": "Question 1",
                "grade_event_id": "grade-1",
                "is_correct": False,
                "grade_ratio": 0.0,
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 3, 10),
                "course_id": "course-a",
                "user_id": "u-1",
                "problem_id": "problem-1",
                "module_usage_key": "problem-1",
                "module_display_name": "Question 1",
                "grade_event_id": "grade-2",
                "is_correct": True,
                "grade_ratio": 1.0,
            },
        ]
    )
    metric_row = build_gold_exam_question_metrics(
        exam_windows_df,
        server_submissions_df,
        grades_df,
    ).collect()[0]

    assert metric_row.exam_attempt_id == "attempt-1"
    assert metric_row.problem_id == "problem-1"
    assert metric_row.submission_event_count == 2
    assert metric_row.max_attempt_no == 2
    assert metric_row.grade_event_count == 2
    assert metric_row.wrong_grade_event_count == 1
    assert metric_row.correct_grade_event_count == 1
    assert metric_row.final_is_correct is True
    assert metric_row.final_grade_ratio == 1.0


def test_build_exam_windows_handles_session_changes_within_same_attempt(spark) -> None:
    exam_attempts_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "course_id": "course-a",
                "exam_id": "exam-1",
                "exam_name": "Final exam",
                "attempt_id": "attempt-2",
                "user_id": "u-2",
                "session_id": "session-start",
                "attempt_event_type": "edx.special_exam.timed.attempt.started",
                "started_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "submitted_time_utc": None,
            },
            {
                "event_time_utc": datetime(2026, 1, 18, 20, 10, 5),
                "course_id": "course-a",
                "exam_id": "exam-1",
                "exam_name": "Final exam",
                "attempt_id": "attempt-2",
                "user_id": "u-2",
                "session_id": "session-submit",
                "attempt_event_type": "edx.special_exam.timed.attempt.submitted",
                "started_time_utc": datetime(2026, 1, 18, 20, 0, 0),
                "submitted_time_utc": datetime(2026, 1, 18, 20, 10, 0),
            },
        ]
    )

    exam_windows = build_exam_windows(exam_attempts_df).collect()

    assert len(exam_windows) == 1
    assert exam_windows[0].exam_attempt_id == "attempt-2"
    assert exam_windows[0].session_id == "session-submit"
    assert exam_windows[0].window_start_utc == datetime(2026, 1, 18, 20, 0, 0)
    assert exam_windows[0].window_end_utc == datetime(2026, 1, 18, 20, 10, 0)
    assert exam_windows[0].is_submitted is True
