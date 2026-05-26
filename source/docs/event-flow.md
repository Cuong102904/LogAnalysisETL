# Event Flow

## End-to-End Flow

1. `tracking_log_replayer` đọc JSON lines từ `BK_activity_logs_unzipped`.
2. Producer publish raw lines vào Kafka `mooc.raw.events`.
3. Bronze app đọc stream, enrich metadata, technical dedup, ghi `bronze.mooc_events_raw`.
4. Silver app đọc bronze stream/table, classify theo `configs/rules/classification.yaml`.
5. Silver normalizers tạo các silver tables theo domain.
6. Gold app tổng hợp feature tables như `gold.video_friction_signals` và `gold.exam_integrity_signals`.
7. Event lỗi parse hoặc vi phạm required tối thiểu sẽ đi `mooc.dlq.events` hoặc `silver.unknown_events`.

## Ingest Coverage Notes

- Producer allowlist tại `kafka/config/producer_filter.yaml` đã mở rộng để thu thêm:
  - `pdf/book` interactions
  - navigation/page movement
  - problem/quiz, grade/progress
  - access/login/dashboard/session
  - completion-related events
- Mục tiêu là tăng độ phủ raw events cho phân tích hành vi, đồng thời vẫn giữ DLQ cho dữ liệu nhiễu/không khớp rule.

## Unknown/Dead-letter Strategy

- Invalid JSON tại ingress: gửi Kafka DLQ, payload giữ `raw` và `raw_value` để tra ngược nội dung gốc.
- Parse được nhưng không match rule nghiệp vụ: ghi `silver.unknown_events`.
- DLQ Kafka giữ thêm `event_snapshot` để biết event nào bị loại, đồng thời giữ `raw`/`raw_value` để debug nhanh.
- Hỗ trợ replay bằng cách đọc lại raw topic hoặc bronze table theo partition thời gian.
