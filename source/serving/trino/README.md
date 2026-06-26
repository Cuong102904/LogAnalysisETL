# Trino Serving Assets

Trino is the SQL query layer for the lakehouse semantic layer.

## What is implemented

- Compose has a `trino` service and a `trino-bootstrap` service.
- Trino image and config live under `serving/trino/`.
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
- Trino 455 uses the native S3 file system, enabled with `fs.native-s3.enabled=true`.
- The bootstrap registers the Gold Delta tables needed by the semantic layer under `s3://lakehouse/learnlake/gold/...`.
- Views are stored as SQL files so they can be bootstrapped from CLI or consumed by Superset SQL Lab.
