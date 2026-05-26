from datetime import date, datetime

from domain.gold.analytics import (
    _rolling_anomaly_score,
    build_behavior_anomalies,
    build_exam_anomaly_features,
    build_learning_journey_features,
    build_pdf_behavior_features,
    build_quiz_performance_features,
    build_video_anomaly_features,
)


def _video_row(day: int, user_id: int, session_id: str) -> dict[str, object]:
    return {
        "event_date": date(2026, 1, day),
        "course_id": "course-a",
        "video_id": "video-1",
        "event_type": "play_video",
        "user_id": user_id,
        "session_id": session_id,
        "time": datetime(2026, 1, day, 9, 0, 0),
        "current_time": 20.0,
        "video_duration": 120.0,
        "old_time": 0.0,
        "new_time": 0.0,
    }


def _pdf_row(day: int, event_type: str, page_number: int, scroll_direction: str | None) -> dict[str, object]:
    return {
        "event_date": date(2026, 1, day),
        "course_id": "course-a",
        "user_id": 1 if day < 3 else 2,
        "session_id": f"sess-{day}",
        "time": datetime(2026, 1, day, 10, 0, 0),
        "event_type": event_type,
        "chapter": "chapter-1",
        "pdf_name": "book.pdf",
        "page_number": page_number,
        "scroll_direction": scroll_direction,
        "scale_amount": 1.25 if "scaled" in event_type else 0.0,
        "doc_url": "",
    }


def _performance_row(day: int, event_type: str, earned: float, possible: float) -> dict[str, object]:
    return {
        "event_date": date(2026, 1, day),
        "course_id": "course-a",
        "user_id": 1 if day < 3 else 2,
        "session_id": f"quiz-{day}",
        "time": datetime(2026, 1, day, 11, 0, 0),
        "event_type": event_type,
        "problem_id": "prob-1",
        "problem_type": "mcq",
        "weighted_earned": earned,
        "weighted_possible": possible,
    }


def _journey_row(day: int, event_type: str, block_type: str, block_id: str, completion_value: float) -> dict[str, object]:
    return {
        "event_date": date(2026, 1, day),
        "course_id": "course-a",
        "user_id": 1 if day < 4 else 2,
        "session_id": f"journey-{day}",
        "time": datetime(2026, 1, day, 12, 0, 0),
        "event_type": event_type,
        "event_hour": 12,
        "block_type": block_type,
        "block_id": block_id,
        "completion_value": completion_value,
    }


def test_video_anomaly_features_detects_spike(spark) -> None:
    rows = [_video_row(day, 100 + day, f"s-{day}") for day in range(1, 6)]
    rows.extend(_video_row(6, 200 + (idx % 2), f"s-6-{idx}") for idx in range(120))

    result = build_video_anomaly_features(spark.createDataFrame(rows)).orderBy("event_date").collect()

    assert len(result) == 6
    assert result[-1]["event_count"] == 120
    assert result[-1]["distinct_users"] == 2
    assert result[-1]["anomaly_domain"] == "video"
    assert result[-1]["is_anomaly"] is True


def test_exam_anomaly_features_detects_multi_ip_login(spark) -> None:
    exam_rows = [
        {
            "event_date": date(2026, 1, 17),
            "course_id": "course-a",
            "exam_id": 501,
            "exam_content_id": "exam-block-1",
            "exam_name": "final exam",
            "attempt_id": 9001,
            "attempt_user_id": 294324,
            "user_id": 294324,
            "username": "202416597",
            "time_limit_mins": 60,
            "allowed_time_limit_mins": 60,
            "exam_is_proctored": True,
            "exam_is_practice_exam": False,
            "exam_is_active": True,
            "attempt_started_at": datetime(2026, 1, 17, 21, 0, 0),
            "attempt_completed_at": datetime(2026, 1, 17, 21, 55, 0),
            "session_id": "sess-a",
            "ip": "116.96.44.103",
            "time": datetime(2026, 1, 17, 21, 22, 17),
            "attempt_event": "created",
            "elapsed_time_secs": 120,
            "attempt_code": "A1",
        },
        {
            "event_date": date(2026, 1, 17),
            "course_id": "course-a",
            "exam_id": 501,
            "exam_content_id": "exam-block-1",
            "exam_name": "final exam",
            "attempt_id": 9001,
            "attempt_user_id": 294324,
            "user_id": 294324,
            "username": "202416597",
            "time_limit_mins": 60,
            "allowed_time_limit_mins": 60,
            "exam_is_proctored": True,
            "exam_is_practice_exam": False,
            "exam_is_active": True,
            "attempt_started_at": datetime(2026, 1, 17, 21, 0, 0),
            "attempt_completed_at": datetime(2026, 1, 17, 21, 55, 0),
            "session_id": "sess-a",
            "ip": "116.96.44.103",
            "time": datetime(2026, 1, 17, 21, 24, 0),
            "attempt_event": "submitted",
            "elapsed_time_secs": 3300,
            "attempt_code": "A1",
        },
    ]
    system_rows = [
        {
            "event_date": date(2026, 1, 17),
            "course_id": "course-a",
            "user_id": 294324,
            "username": "202416597",
            "session_id": "sess-a",
            "ip": "116.96.44.103",
            "time": datetime(2026, 1, 17, 21, 25, 0),
            "event_type": "/api/edx_proctoring/v1/proctored_exam/attempt",
            "path": "/api/edx_proctoring/v1/proctored_exam/attempt",
        },
        {
            "event_date": date(2026, 1, 17),
            "course_id": "course-a",
            "user_id": 294324,
            "username": "202416597",
            "session_id": "sess-b",
            "ip": "42.118.16.158",
            "time": datetime(2026, 1, 17, 21, 30, 0),
            "event_type": "login",
            "path": "/login",
        },
        {
            "event_date": date(2026, 1, 17),
            "course_id": "course-a",
            "user_id": 294324,
            "username": "202416597",
            "session_id": "sess-c",
            "ip": "27.72.147.141",
            "time": datetime(2026, 1, 17, 21, 33, 0),
            "event_type": "proctoring",
            "path": "/proctoring",
        },
    ]

    result = build_exam_anomaly_features(
        spark.createDataFrame(exam_rows),
        spark.createDataFrame(system_rows),
    ).collect()

    anomaly_types = {row["anomaly_type"] for row in result}
    assert anomaly_types == {"attempt_activity", "multi_ip_login"}

    multi_ip = next(row for row in result if row["anomaly_type"] == "multi_ip_login")
    assert multi_ip["distinct_login_ips"] == 3
    assert multi_ip["is_anomaly"] is True


def test_rolling_anomaly_score_uses_bucket_order_for_video(spark) -> None:
    rows = [
        {
            "event_date": date(2026, 1, 1),
            "course_id": "course-a",
            "video_id": "video-1",
            "action_type": "play_video",
            "time_bucket_s": bucket,
            "wallclock_bucket_ts": datetime(2026, 1, 1, 9, 0, 0),
            "event_count": 1 if bucket < 30 else 10,
        }
        for bucket in (0, 5, 10, 15, 20, 25, 30)
    ]

    result = _rolling_anomaly_score(
        spark.createDataFrame(rows),
        ["course_id", "video_id", "action_type"],
        "event_count",
        ["event_date", "time_bucket_s", "wallclock_bucket_ts"],
    ).orderBy("time_bucket_s").collect()

    assert result[-1]["time_bucket_s"] == 30
    assert result[-1]["rolling_mean_7"] == 3.25


def test_pdf_quiz_and_journey_features(spark) -> None:
    pdf_df = spark.createDataFrame(
        [
            _pdf_row(6, "textbook.pdf.page.scrolled", 3, "down"),
            _pdf_row(6, "textbook.pdf.display.scaled", 3, None),
        ]
    )
    pdf_result = build_pdf_behavior_features(pdf_df).collect()
    assert len(pdf_result) == 1
    assert pdf_result[0]["event_count"] == 2
    assert pdf_result[0]["scroll_count"] == 1
    assert pdf_result[0]["zoom_count"] == 1
    assert pdf_result[0]["scroll_balance"] == 1

    performance_df = spark.createDataFrame(
        [
            _performance_row(6, "edx.grades.problem.submitted", 0.5, 1.0),
            _performance_row(6, "problem_check", 0.0, 1.0),
        ]
    )
    performance_result = build_quiz_performance_features(performance_df).collect()
    assert len(performance_result) == 1
    assert performance_result[0]["event_count"] == 2
    assert performance_result[0]["submit_count"] == 1
    assert performance_result[0]["check_count"] == 1
    assert performance_result[0]["attempt_count"] == 2

    learning_df = spark.createDataFrame(
        [
            _journey_row(6, "play_video", "video", "video-1", 0.0),
            _journey_row(6, "textbook.pdf.page.scrolled", "chapter", "chapter-1", 1.0),
            _journey_row(6, "seq_next", "sequence", "seq-1", 0.0),
        ]
    )
    journey_result = build_learning_journey_features(learning_df).collect()
    assert len(journey_result) == 1
    assert journey_result[0]["event_count"] == 3
    assert journey_result[0]["video_event_count"] == 1
    assert journey_result[0]["pdf_event_count"] == 1
    assert journey_result[0]["navigation_event_count"] == 1
    assert journey_result[0]["completion_event_count"] == 1


def test_behavior_anomalies_union_contains_all_domains(spark) -> None:
    video_df = spark.createDataFrame(
        [_video_row(day, 100 + day, f"s-{day}") for day in range(1, 6)]
        + [_video_row(6, 200 + (idx % 2), f"s-6-{idx}") for idx in range(120)]
    )
    pdf_df = spark.createDataFrame(
        [
            _pdf_row(1, "textbook.pdf.page.scrolled", 1, "down"),
            _pdf_row(2, "textbook.pdf.page.scrolled", 1, "down"),
            _pdf_row(3, "textbook.pdf.page.scrolled", 1, "down"),
            _pdf_row(4, "textbook.pdf.page.scrolled", 1, "down"),
            _pdf_row(5, "textbook.pdf.page.scrolled", 1, "down"),
            _pdf_row(6, "textbook.pdf.page.scrolled", 1, "down"),
            _pdf_row(6, "textbook.pdf.display.scaled", 2, None),
        ]
    )
    performance_df = spark.createDataFrame(
        [
            _performance_row(1, "problem_check", 0.0, 1.0),
            _performance_row(2, "problem_check", 0.0, 1.0),
            _performance_row(3, "problem_check", 0.0, 1.0),
            _performance_row(4, "problem_check", 0.0, 1.0),
            _performance_row(5, "problem_check", 0.0, 1.0),
            _performance_row(6, "edx.grades.problem.submitted", 1.0, 1.0),
            _performance_row(6, "edx.grades.problem.submitted", 1.0, 1.0),
            _performance_row(6, "problem_check", 0.0, 1.0),
        ]
    )
    learning_df = spark.createDataFrame(
        [
            _journey_row(1, "play_video", "video", "video-1", 0.0),
            _journey_row(2, "textbook.pdf.page.scrolled", "chapter", "chapter-1", 1.0),
            _journey_row(3, "seq_next", "sequence", "seq-1", 0.0),
            _journey_row(4, "problem_check", "problem", "prob-1", 0.0),
            _journey_row(5, "play_video", "video", "video-2", 0.0),
            _journey_row(6, "play_video", "video", "video-2", 0.0),
            _journey_row(6, "play_video", "video", "video-2", 0.0),
        ]
    )

    anomalies = build_behavior_anomalies(video_df, pdf_df, performance_df, learning_df).collect()
    domains = {row["anomaly_domain"] for row in anomalies}

    assert domains == {"video", "pdf", "performance", "journey"}
    assert any(row["is_anomaly"] for row in anomalies)
