# Trino Serving Assets

Trino is the SQL query layer for the lakehouse semantic layer.

## What is implemented

- Compose has a `trino` service and a `trino-bootstrap` service.
- Trino image and config live under `serving/trino/`.
- Hive Metastore is self-hosted in the compose stack.
- Direct Bronze/Silver Delta tables and the current exam-ops Gold Delta tables are registered into `delta.mooc`.
- Semantic views live in `views/`.
- Bootstrap service `trino-bootstrap` waits for Trino, registers direct tables, and applies semantic views.

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
- The bootstrap registers the Bronze/Silver tables plus `gold_exam_load_10s`, `gold_exam_attempt_flow_10s`, `gold_exam_attempt_timeline`, and `gold_exam_question_metrics` under `s3a://lakehouse/learnlake/...`.
- Views are stored as SQL files so they can be bootstrapped from CLI or consumed by Superset SQL Lab.
