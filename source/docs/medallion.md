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

- Gold được chia thành 2 cadence:
  - `streaming`: `gold.video_friction_signals`, `gold.exam_integrity_signals`, `gold.behavior_anomaly_signals`
  - `batch`: `gold.pdf_engagement_features`, `gold.quiz_attempt_metrics`, `gold.user_learning_profile_daily`
- `gold.anomaly_alerts` là stream dẫn xuất từ `gold.behavior_anomaly_signals`, giữ watermark + dedup riêng cho alerting.
- Gold chỉ consume từ Silver, không đọc bronze/Kafka/raw.
- Superset không đọc physical gold tables trực tiếp; nó đọc semantic views trong Trino:
  - `video_friction_view`
  - `exam_anomaly_view`
  - `pdf_engagement_view`
  - `quiz_difficulty_view`
  - `course_improvement_view`
  - `learner_health_view`
  - `behavior_anomaly_view`
  - `alert_events_view`
- Dashboard serving split:
  - `Live Ops`: one dashboard with multiple sections/tabs, auto-refresh around 30 seconds, manual refresh allowed.
  - `Learning Analytics`: manual refresh only after batch jobs finish.
- If Superset bootstrap/API does not materialize the declared dashboards automatically, the repo can fall back to Playwright to create the dashboard surfaces and sections.
