# Trino Query Layer

Trino is the SQL query layer for the lakehouse semantic layer. It reads Delta tables from MinIO through Hive Metastore and exposes code-defined semantic views for BI and ad-hoc analysis.

## What is implemented

- The Trino image is pinned in `serving/trino/Dockerfile` to `trinodb/trino:455`.
- Catalog and runtime config live in `serving/trino/etc/`.
- `delta` and `hive` catalogs use the native S3 file system with `fs.native-s3.enabled=true` and MinIO-backed S3 storage.
- The `trino-bootstrap` service waits for Trino, registers Delta tables in `delta.mooc`, validates the semantic view contracts, and applies the SQL files in `serving/trino/views/`.

## Registered tables

The bootstrap creates `delta.mooc` without forcing an external schema location, then registers these tables from `s3://lakehouse/learnlake/gold/...` locations:

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

`serving/trino/views/00_create_semantic_schema.sql` creates the semantic schema, and the remaining SQL files define the views above. Those view files now read only gold semantic tables, not silver tables, so Superset stays on the BI contract.

## Notes

- Superset connects to Trino through `trino://superset@trino:8080/delta/mooc`.
- The semantic layer is code-driven, so the bootstrap can rebuild it from the SQL files instead of manual UI configuration.
- If the Superset API/bootstrap path does not create dashboards automatically, the dashboard layer has a Playwright fallback that logs in and creates the declared surfaces.
- If the repo is bootstrapped locally outside the container, `TRINO_VIEWS_DIR` can point validation at the checked-in `serving/trino/views/` directory.
