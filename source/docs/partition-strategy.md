# Partition Strategy

## Bronze

- Partition: `ingest_date`, `ingest_hour`.
- Mục tiêu: replay nhanh theo thời gian ingest.

## Silver

- Base partition: `event_date`.
- Bổ sung `course_bucket` (hash) để giảm skew khi course nóng.

## Gold

- `gold.video_anomaly_features`: `event_date`, `course_id`.
- Bảng profile khác: partition theo `event_date` và dimension chính.

## Notes

- Tránh partition cardinality quá cao theo user/session.
- Dùng OPTIMIZE/compaction theo lịch batch định kỳ (deferred airflow).
