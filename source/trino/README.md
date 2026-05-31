# Trino Repository

Trino is the SQL query layer for the lakehouse semantic layer.

## What is implemented

- Compose has a `trino` service and a `trino-bootstrap` service.
- Trino image and config live under `trino/`.
- Hive Metastore is self-hosted in the compose stack.
- Semantic views live in `views/`.
- Bootstrap service `trino-bootstrap` waits for Trino, registers Delta tables, and applies semantic views.

## Semantic Layer

- `video_friction_view`
- `exam_anomaly_view`
- `pdf_engagement_view`
- `quiz_difficulty_view`
- `course_improvement_view`
- `learner_health_view`
- `behavior_anomaly_view`
- `alert_events_view`

## Notes

- Trino uses Hive Metastore plus MinIO-backed S3 storage for the Delta catalog.
- The bootstrap currently registers `silver_video_interactions` and the Gold Delta tables needed by the semantic layer.
- Views are stored as SQL files so they can be bootstrapped from CLI or consumed by Superset SQL Lab.
