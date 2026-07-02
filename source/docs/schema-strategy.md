# Schema Strategy

## Bronze

- Bronze giữ `raw_payload` string + metadata kỹ thuật.
- Một row Bronze tương ứng một event JSON hoàn chỉnh.
- Bronze không parse business schema trước.

## Silver

Silver được tách thành:

- `events_canonical`
- `problem_submissions`
- `problem_grades`
- `exam_attempts`
- `video_interactions`
- `navigation_events`
- `content_access_events`
- `system_noise_events`
- `silver_unknown_events`
- `silver_invalid_events`

## Canonical Rules

- `events_canonical` là bảng classified-event trung tâm.
- `event_time_utc` lấy từ source event `time`, không dùng ingest time cho phân tích hành vi.
- `event_id` là stable event identity đi xuyên qua canonical, domain, unknown, invalid.
- Unknown event không vào canonical.

## Parser Rules

- Shared parse và domain parse đều phải được viết bằng Spark DataFrame/Spark SQL expressions.
- Driver chỉ compile plan; không parse từng row bằng Python.
- Python UDF chỉ dùng khi built-in Spark không biểu diễn nổi logic cần thiết.

## Exam Attempt Strategy

- `problem_submissions`, `problem_grades`, `video_interactions`, `navigation_events`, `content_access_events`, `system_noise_events` là append-style event tables.
- `exam_attempts` là entity-level merged table keyed by `exam_attempt_id`.
