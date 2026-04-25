# Dedup Strategy

## 1) Technical Dedup (Bronze)

- Key: `dedup_key = sha256(topic|partition|offset)` với fallback theo payload+timestamp.
- Áp dụng stream dedup với watermark.

## 2) Stream/Window Dedup (Silver)

- Mỗi luồng dùng watermark event-time.
- Dùng key theo domain + timestamp làm tròn để giảm duplicate burst.

## 3) Business Dedup (Silver/Gold)

- `performance`: theo `event_transaction_id`.
- `video_interactions`: theo user/video/action trong ngưỡng thời gian ngắn.
- Rule threshold và key nằm trong `spark/configs/rules/dedup.yaml`.
