# Medallion Lakehouse Responsibilities

## Bronze

- Ingest stream từ Kafka `mooc.raw.events`.
- Lưu `value_raw` nguyên bản + `time` parse từ raw event + ingest metadata.
- Tạo `dedup_key` technical, parse status, parse error.
- Chỉ làm technical dedup nhẹ để giữ fidelity dữ liệu.

## Silver

- Chuẩn hóa một raw source event thành:
  - `silver_event_index`
  - zero hoặc nhiều domain facts theo semantic domain
    - `silver_assessment_events`
    - `silver_video_events`
    - `silver_document_events`
    - `silver_navigation_events`
    - `silver_exam_events`
    - `silver_course_content_events`
    - `silver_authoring_events`
    - `silver_auth_events`
    - `silver_system_events`
    - `silver_unknown_events`
- `silver_event_index` giữ lineage, common dimensions, normalized type, quality status.
- Domain fact tables chỉ giữ typed analytics-ready fields cho từng domain.
- `silver_invalid_events` giữ record fail contract hoặc quality.
- `time` là event-time canonical dùng cho analytics và watermark/dedup.
- Áp dụng stream/window dedup và business dedup theo rule config.

## Gold

- Tổng hợp profile phục vụ analytics:
  - `gold.user_learning_profile` (skeleton)
  - `gold.problem_performance` (skeleton)
  - `gold.system_profile` (skeleton)
  - `gold.video_friction_signals` (implemented)
  - `gold.pdf_engagement_features` (implemented)
  - `gold.quiz_attempt_metrics` (implemented)
  - `gold.user_learning_profile_daily` (implemented)
- Chuẩn bị anomaly signals cho dashboard và rule engine:
  - `gold.exam_integrity_signals` (implemented)
  - `gold.behavior_anomaly_signals` (implemented)
