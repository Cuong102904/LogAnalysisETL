# Classification Rules

Rule được đặt trong `spark/configs/rules/classification.yaml`.

## Nhóm chính

- `learning`: `event_source=browser` và `event_type` chứa `video` hoặc `seq`.
- `performance`: `event_source=server` và `event_type=edx.grades.problem.submitted`.
- `system`: event liên quan `proctoring`, `heartbeat`, `auth`, `login`, `session`.
- `unknown`: fallback khi không match các rule trên.

## Nguyên tắc

- Mọi rule là config-driven.
- Code classifier chỉ compile rule thành Spark expression.
- Có thể thêm rule mới mà không đổi logic lõi.
