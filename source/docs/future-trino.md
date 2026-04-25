# Future Plan for Trino

Trạng thái hiện tại: deferred runtime.

## Mục tiêu phase sau

- Mount Delta tables từ MinIO qua catalog connector.
- Cung cấp truy vấn ad-hoc cho Silver/Gold.
- Chuẩn hóa semantic layer cho BI tools.

## Định hướng

- Tạo `delta.properties` trỏ bucket MinIO và metastore phù hợp.
- Thiết kế view business-friendly cho `gold.*`.
- Bổ sung benchmark query và governance policies.
