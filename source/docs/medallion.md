# Medallion Lakehouse Responsibilities

## Bronze

- Ingest stream từ Kafka `mooc.raw.events`.
- Lưu `value_raw` nguyên bản + `time` parse từ raw event + ingest metadata.
- Tạo `dedup_key` technical, parse status, parse error.
- Chỉ làm technical dedup nhẹ để giữ fidelity dữ liệu.

## Silver

- Chuẩn hóa và phân loại event thành:
  - `silver.learning_events`
  - `silver.performance_events`
  - `silver.exam_attempts`
  - `silver.system_events`
  - `silver.unknown_events`
  - `silver.video_interactions`
- Áp dụng schema permissive, xử lý schema drift, parse nested `event`.
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
