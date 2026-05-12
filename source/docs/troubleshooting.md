# Troubleshooting

## Kafka

- Không consume được: kiểm tra `bootstrap.servers` và topic tồn tại.
- Dữ liệu không vào silver: kiểm tra bronze app đã chạy và checkpoint path.

## MinIO / Delta

- Lỗi 403: kiểm tra access key/secret key và bucket policy.
- Lỗi checkpoint lock: dọn checkpoint stale khi test local.

## Schema

- Parse lỗi nhiều: xem tỷ lệ `parse_status` ở bronze.
- Drift mới: update schema permissive và normalizer tương ứng.

## Performance

- Skew tại một course: kiểm tra `course_bucket`.
- Microbatch chậm: giảm trigger interval hoặc tối ưu partition writes.
