# Trino Query Layer

Trino is the SQL query layer for the lakehouse semantic layer. It reads Delta tables from MinIO through Hive Metastore and exposes code-defined semantic views for BI and ad-hoc analysis.

## What is implemented

- The Trino image is pinned in `serving/trino/Dockerfile` to `trinodb/trino:455`.
- Catalog and runtime config live in `serving/trino/etc/`.
- `delta` catalog uses the Delta Lake connector with Hive Metastore and MinIO-backed S3 storage.
- `hive` catalog is also configured for Hive Metastore access and non-managed table writes.
- The `trino-bootstrap` service waits for Trino, registers Delta tables in `delta.mooc`, and applies the SQL files in `serving/trino/views/`.

## Registered tables

The bootstrap currently registers these tables in the `delta.mooc` schema:

- `silver_video_interactions`
- `video_friction_signals`
- `pdf_engagement_features`
- `quiz_attempt_metrics`
- `user_learning_profile_daily`
- `exam_integrity_signals`
- `behavior_anomaly_signals`
- `anomaly_alerts`

## Semantic views

- `video_friction_view`
- `exam_anomaly_view`
- `pdf_engagement_view`
- `quiz_difficulty_view`
- `course_improvement_view`
- `learner_health_view`
- `behavior_anomaly_view`
- `alert_events_view`

`serving/trino/views/00_create_semantic_schema.sql` creates the semantic schema, and the remaining SQL files define the views above.

## Notes

- Superset connects to Trino through `trino://superset@trino:8080/delta/mooc`.
- The semantic layer is code-driven, so the bootstrap can rebuild it from the SQL files instead of manual UI configuration.
