# Event Flow

## End-to-End Flow

1. `tracking_log_replayer` đọc JSON lines từ `BK_activity_logs_unzipped`.
2. Producer publish raw lines vào Kafka `mooc.raw.events`.
3. Bronze app đọc stream, enrich metadata, technical dedup, ghi `bronze.mooc_events_raw`.
4. Silver app đọc bronze stream/table, parse `event`, route theo source-pack YAML, và tạo `silver_event_index`.
5. Silver extractor fan-out ra các silver tables theo domain.
6. Gold app tổng hợp feature tables như `gold.video_friction_signals` và `gold.exam_integrity_signals`.
7. Event lỗi parse hoặc vi phạm required tối thiểu sẽ đi `mooc.dlq.events` hoặc `silver_invalid_events`.
8. Event parse được nhưng chưa match source-pack route sẽ vẫn có `silver_event_index` row và `silver_unknown_events` row.

## Ingest Coverage Notes

- Producer allowlist tại `platform/local/kafka/config/producer_filter.yaml` đã mở rộng để thu thêm:
  - `pdf/book` interactions
  - navigation/page movement
  - problem/quiz, grade/progress
  - access/login/dashboard/session
  - completion-related events
- Mục tiêu là tăng độ phủ raw events cho phân tích hành vi, đồng thời vẫn giữ DLQ cho dữ liệu nhiễu/không khớp rule.

## Unknown/Dead-letter Strategy

- Invalid JSON tại ingress: gửi Kafka DLQ, payload giữ `raw` và `raw_value` để tra ngược nội dung gốc.
- Parse được nhưng không match rule nghiệp vụ: ghi `silver_unknown_events` và giữ lineage trong `silver_event_index`.
- Fail contract/required quality rule tại Silver: ghi `silver_invalid_events` và không emit domain facts.
- DLQ Kafka giữ thêm `event_snapshot` để biết event nào bị loại, đồng thời giữ `raw`/`raw_value` để debug nhanh.
- Hỗ trợ replay bằng cách đọc lại raw topic hoặc bronze table theo partition thời gian.
