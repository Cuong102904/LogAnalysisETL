# Event Flow

## End-to-End Flow

1. `tracking_log_replayer` đọc JSON lines từ `BK_activity_logs_unzipped`.
2. Producer publish raw lines vào Kafka `mooc.raw.events`.
3. Bronze app đọc stream, enrich metadata, technical dedup, ghi `bronze.mooc_events_raw`.
4. Silver app đọc bronze stream/table, classify theo `configs/rules/classification.yaml`.
5. Silver normalizers tạo các silver tables theo domain.
6. Gold app tổng hợp feature tables, ưu tiên `gold.video_anomaly_features`.
7. Event lỗi parse hoặc vi phạm required tối thiểu sẽ đi `mooc.dlq.events` hoặc `silver.unknown_events`.

## Unknown/Dead-letter Strategy

- Invalid JSON tại ingress: gửi Kafka DLQ.
- Parse được nhưng không match rule nghiệp vụ: ghi `silver.unknown_events`.
- Hỗ trợ replay bằng cách đọc lại raw topic hoặc bronze table theo partition thời gian.
