# Future Plan for Airflow

Trạng thái hiện tại: deferred runtime.

## Mục tiêu phase sau

- Điều phối Bronze -> Silver -> Gold theo lịch.
- Quản lý SLA, retry, alerting cho từng stage.
- Backfill theo partition date cho trường hợp replay.

## DAG đề xuất

- `bronze_ingest_stream_healthcheck`
- `silver_transform_incremental`
- `gold_aggregate_incremental`
- `data_quality_checks`
- `publish_metrics`

## Notes

- Giai đoạn hiện tại chỉ giữ skeleton và tài liệu định hướng.
