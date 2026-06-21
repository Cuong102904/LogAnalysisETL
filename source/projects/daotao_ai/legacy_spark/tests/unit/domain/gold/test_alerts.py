from datetime import date, datetime

from domain.gold.alerts import build_alert_events


def test_build_alert_events_filters_and_labels_severity(spark) -> None:
    rows = [
        {
            "anomaly_domain": "video",
            "entity_type": "video_action",
            "entity_id": "video-1|pause_video",
            "event_date": date(2026, 1, 6),
            "course_id": "course-a",
            "metric_name": "event_count",
            "metric_value": 120.0,
            "event_count": 120,
            "distinct_users": 12,
            "distinct_sessions": 15,
            "first_time": datetime(2026, 1, 6, 9, 0, 0),
            "last_time": datetime(2026, 1, 6, 9, 5, 0),
            "last_event_time": datetime(2026, 1, 6, 9, 5, 0),
            "rolling_mean_7": 20.0,
            "rolling_std_7": 10.0,
            "z_score": 5.2,
            "is_anomaly": True,
        },
        {
            "anomaly_domain": "pdf",
            "entity_type": "pdf_content",
            "entity_id": "book-1",
            "event_date": date(2026, 1, 6),
            "course_id": "course-a",
            "metric_name": "event_count",
            "metric_value": 2.0,
            "event_count": 2,
            "distinct_users": 1,
            "distinct_sessions": 1,
            "first_time": datetime(2026, 1, 6, 10, 0, 0),
            "last_time": datetime(2026, 1, 6, 10, 1, 0),
            "last_event_time": datetime(2026, 1, 6, 10, 1, 0),
            "rolling_mean_7": 1.0,
            "rolling_std_7": 0.5,
            "z_score": 1.0,
            "is_anomaly": True,
        },
    ]

    result = build_alert_events(spark.createDataFrame(rows)).collect()

    assert len(result) == 1
    alert = result[0]
    assert alert["alert_domain"] == "video"
    assert alert["alert_type"] == "video_anomaly"
    assert alert["alert_severity"] == "critical"
    assert alert["alert_message"].startswith("Anomaly detected for video")
    assert alert["alert_time"] == datetime(2026, 1, 6, 9, 5, 0)
