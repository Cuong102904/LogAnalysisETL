from datetime import datetime
import json

from domain.silver.normalizers.exam_attempts import normalize_exam_attempts


def test_normalize_exam_attempts_keeps_ip(spark) -> None:
    raw = spark.createDataFrame(
        [
            {
                "dedup_key": "k1",
                "kafka_timestamp": datetime(2026, 1, 17, 21, 22, 17),
                "ingest_ts": datetime(2026, 1, 17, 21, 22, 18),
                "silver_class": "exam",
                "value_raw": json.dumps(
                    {
                        "time": "2026-01-17T21:22:17.049637+00:00",
                        "event_type": "edx.special_exam.timed.attempt.started",
                        "username": "202416597",
                        "session": "5480a90882832e5ee99e7baa6b472d45",
                        "ip": "116.96.44.103",
                        "context": {
                            "user_id": 294324,
                            "course_id": "course-v1:SoDiTEC+Dsa01+2025_1",
                        },
                        "event": {
                            "attempt_id": "9001",
                            "attempt_user_id": "294324",
                            "exam_id": "501",
                            "exam_content_id": "block-v1:SoDiTEC+Dsa01+2025_1+type@problem+block@4a49b88c6e3da158c21b",
                            "exam_name": "final exam",
                            "exam_is_proctored": "true",
                            "exam_is_practice_exam": "false",
                            "exam_is_active": "true",
                            "exam_default_time_limit_mins": "60",
                            "attempt_allowed_time_limit_mins": "60",
                            "attempt_started_at": "2026-01-17T21:00:00+00:00",
                            "attempt_status": "started",
                            "attempt_event_elapsed_time_secs": "120",
                            "attempt_code": "ABC123",
                        },
                    }
                ),
            }
        ]
    )

    result = normalize_exam_attempts(raw).collect()

    assert len(result) == 1
    assert result[0]["ip"] == "116.96.44.103"
    assert result[0]["attempt_event"] == "started"
