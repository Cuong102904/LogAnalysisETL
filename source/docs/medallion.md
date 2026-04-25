# Medallion Lakehouse Responsibilities

## Bronze

- Ingest stream từ Kafka `mooc.raw.events`.
- Lưu `value_raw` nguyên bản + Kafka metadata + ingest metadata.
- Tạo `dedup_key` technical, parse status, parse error.
- Chỉ làm technical dedup nhẹ để giữ fidelity dữ liệu.

## Silver

- Chuẩn hóa và phân loại event thành:
  - `silver.learning_events`
  - `silver.performance_events`
  - `silver.system_events`
  - `silver.unknown_events`
  - `silver.video_interactions`
- Áp dụng schema permissive, xử lý schema drift, parse nested `event`.
- Áp dụng stream/window dedup và business dedup theo rule config.

## Gold

- Tổng hợp profile phục vụ analytics:
  - `gold.user_learning_profile` (skeleton)
  - `gold.problem_performance` (skeleton)
  - `gold.system_profile` (skeleton)
  - `gold.video_anomaly_features` (implemented)
- Chuẩn bị semantic metrics cho dashboard và anomaly rule.
