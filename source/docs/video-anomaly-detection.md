# Video Anomaly Detection Design

Mục tiêu: phát hiện mật độ tua/dừng/xem lại đột biến tại thời điểm cụ thể trong video từ nhiều người học.

## Silver Model

Nguồn chính: `silver.video_interactions`.

Các cột cốt lõi:
- `course_id`, `video_id`
- `time`, `event_date`
- `action_type` (`play`, `pause`, `seek`, `stop`, `speed_change`, `load`)
- `current_time_s`, `from_time_s`, `to_time_s`
- `user_id_int`, `session`

## Gold Feature Table

Đích: `gold.video_friction_signals`.

Grain:
- `course_id + video_id + time_bucket_s + wallclock_bucket_ts + action_type`

Metrics:
- `event_count`
- `distinct_users`
- `distinct_sessions`
- `surge_score`

## Rule-based Anomaly

- Bucketing theo `time_bucket_s` (default 5 giây).
- So sánh mỗi bucket với baseline rolling lân cận.
- Rule gợi ý:
  - `distinct_users >= min_distinct_users`
  - `surge_score >= surge_z_threshold`

Config đặt tại `spark/configs/rules/video_anomaly.yaml`.

## Dashboard Ideas

- Heatmap theo `time_bucket_s` của video.
- Top video theo `max(surge_score)` trong 24h.
- Drill-down bucket để xem session/user chi tiết.
