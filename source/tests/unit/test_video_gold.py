from __future__ import annotations

import os
from datetime import datetime

import pytest

from learnlake.runtime import build_spark
from projects.daotao_ai.gold.domain.video import (
    build_gold_course_video_seek_hotspots_daily,
    build_gold_course_video_summary_daily,
    build_gold_user_video_engagement,
    build_gold_user_video_engagement_daily,
    build_gold_video_retention_by_bucket_daily,
    build_video_events_base,
)
from projects.daotao_ai.gold.video_config import VideoGoldConfig


@pytest.fixture(scope="session")
def spark():
    previous_openlineage = os.environ.get("OPENLINEAGE_ENABLED")
    os.environ["OPENLINEAGE_ENABLED"] = "false"
    session = build_spark("test_video_gold")
    try:
        yield session
    finally:
        session.stop()
        if previous_openlineage is None:
            os.environ.pop("OPENLINEAGE_ENABLED", None)
        else:
            os.environ["OPENLINEAGE_ENABLED"] = previous_openlineage


def test_video_config_loads_defaults() -> None:
    config = VideoGoldConfig.from_env()

    assert config.app_name == "gold_video_batch"
    assert config.input_video_events_path.endswith("/silver/video_events")
    assert config.output_user_video_engagement_path.endswith("/gold/gold_user_video_engagement")
    assert config.output_course_video_summary_daily_path.endswith("/gold/gold_course_video_summary_daily")


def test_video_gold_tables_aggregate_video_events(spark) -> None:
    events_df = spark.createDataFrame(
        [
            {
                "event_time_utc": datetime(2026, 1, 21, 10, 0, 0),
                "username": "u1",
                "user_id": "1",
                "session_id": "s1",
                "course_id": "course-a",
                "video_id": "video-1",
                "video_block_id": "block-1",
                "video_code": "video-1",
                "action_type": "load_video",
                "duration_s": 120.0,
                "current_time_s": 0.0,
                "old_time_s": None,
                "new_time_s": None,
                "watch_ratio": 0.0,
                "position_bucket_10s": 0,
                "position_bucket_30s": 0,
            },
            {
                "event_time_utc": datetime(2026, 1, 21, 10, 1, 0),
                "username": "u1",
                "user_id": "1",
                "session_id": "s1",
                "course_id": "course-a",
                "video_id": "video-1",
                "video_block_id": "block-1",
                "video_code": "video-1",
                "action_type": "play_video",
                "duration_s": 120.0,
                "current_time_s": 25.0,
                "old_time_s": 20.0,
                "new_time_s": 25.0,
                "watch_ratio": 0.2,
                "position_bucket_10s": 20,
                "position_bucket_30s": 0,
            },
            {
                "event_time_utc": datetime(2026, 1, 21, 10, 2, 0),
                "username": "u1",
                "user_id": "1",
                "session_id": "s1",
                "course_id": "course-a",
                "video_id": "video-1",
                "video_block_id": "block-1",
                "video_code": "video-1",
                "action_type": "seek_video",
                "duration_s": 120.0,
                "current_time_s": 65.0,
                "old_time_s": 25.0,
                "new_time_s": 65.0,
                "watch_ratio": 0.54,
                "position_bucket_10s": 60,
                "position_bucket_30s": 60,
            },
            {
                "event_time_utc": datetime(2026, 1, 21, 10, 3, 0),
                "username": "u1",
                "user_id": "1",
                "session_id": "s1",
                "course_id": "course-a",
                "video_id": "video-1",
                "video_block_id": "block-1",
                "video_code": "video-1",
                "action_type": "pause_video",
                "duration_s": 120.0,
                "current_time_s": 110.0,
                "old_time_s": 105.0,
                "new_time_s": 110.0,
                "watch_ratio": 0.92,
                "position_bucket_10s": 110,
                "position_bucket_30s": 90,
            },
            {
                "event_time_utc": datetime(2026, 1, 21, 10, 4, 0),
                "username": "u2",
                "user_id": "2",
                "session_id": "s2",
                "course_id": "course-a",
                "video_id": "video-1",
                "video_block_id": "block-1",
                "video_code": "video-1",
                "action_type": "seek_video",
                "duration_s": 120.0,
                "current_time_s": 35.0,
                "old_time_s": 50.0,
                "new_time_s": 35.0,
                "watch_ratio": 0.3,
                "position_bucket_10s": 30,
                "position_bucket_30s": 30,
            },
        ]
    )

    base_df = build_video_events_base(events_df)
    base_row = base_df.filter("action_type = 'seek_video'").orderBy("event_time_utc").collect()[0]
    assert base_row.event_date.isoformat() == "2026-01-21"
    assert base_row.seek_direction == "forward"
    assert round(base_row.seek_distance_s, 2) == 40.0

    user_video_rows = {
        (row.user_id, row.course_id, row.video_id): row for row in build_gold_user_video_engagement(base_df).collect()
    }
    assert user_video_rows[("1", "course-a", "video-1")].event_count == 4
    assert user_video_rows[("1", "course-a", "video-1")].seek_count == 1
    assert user_video_rows[("1", "course-a", "video-1")].completed_flag == 1

    daily_user_video_rows = {
        (row.event_date.isoformat(), row.user_id, row.course_id, row.video_id): row
        for row in build_gold_user_video_engagement_daily(base_df).collect()
    }
    assert daily_user_video_rows[("2026-01-21", "1", "course-a", "video-1")].play_count == 1

    summary_rows = {
        (row.event_date.isoformat(), row.course_id, row.video_id): row
        for row in build_gold_course_video_summary_daily(base_df).collect()
    }
    summary = summary_rows[("2026-01-21", "course-a", "video-1")]
    assert summary.active_users == 2
    assert summary.seek_count == 2
    assert summary.completed_users == 1
    assert summary.completion_rate == 0.5

    hotspot_rows = {
        (row.event_date.isoformat(), row.course_id, row.video_id, row.position_bucket_30s): row
        for row in build_gold_course_video_seek_hotspots_daily(base_df).collect()
    }
    assert hotspot_rows[("2026-01-21", "course-a", "video-1", 30)].seek_event_count == 1
    assert hotspot_rows[("2026-01-21", "course-a", "video-1", 60)].seek_event_count == 1
    assert hotspot_rows[("2026-01-21", "course-a", "video-1", 30)].seek_backward_count == 1

    retention_rows = {
        (row.event_date.isoformat(), row.course_id, row.video_id, row.position_bucket_30s): row
        for row in build_gold_video_retention_by_bucket_daily(base_df).collect()
    }
    assert retention_rows[("2026-01-21", "course-a", "video-1", 0)].total_users == 2
    assert retention_rows[("2026-01-21", "course-a", "video-1", 90)].users_reached_bucket == 1
