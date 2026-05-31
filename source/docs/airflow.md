# Airflow Orchestration

Airflow handles operational orchestration for the lakehouse stack. It does not run the continuous Bronze/Silver/Gold streams themselves; those run through Spark and Docker Compose. Airflow focuses on health checks and Delta maintenance jobs.

## What is implemented

- Runtime compose stack is provided by `source/docker-compose.yaml`.
- Reusable Spark wrapper task lives in `tasks/spark_task.py`.
- Bronze runtime checks live in `tasks/bronze/runtime_checks.py`.
- Airflow variable adapter lives in `config/variables.py`.

## Current DAGs

- `bronze_stream_health`
  - checks Kafka readiness,
  - verifies Bronze checkpoint progress,
  - checks Bronze file health.
- `bronze_table_maintenance`
  - runs Delta `OPTIMIZE` on the Bronze table,
  - can optionally run `VACUUM`,
  - reruns post-maintenance health checks.
- Silver maintenance DAGs
  - `silver_maintenance_learning_events`
  - `silver_maintenance_performance_events`
  - `silver_maintenance_exam_attempts`
  - `silver_maintenance_video_interactions`
  - `silver_maintenance_navigation_events`
  - `silver_maintenance_pdf_interactions`
  - `silver_maintenance_system_events`
  - `silver_maintenance_unknown_events`
- Gold maintenance DAG
  - `gold_maintenance_behavior_tables`
  - maintains `video_anomaly_features`, `pdf_behavior_features`, `quiz_performance_features`, `learning_journey_features`, `behavior_anomalies`, and `alert_events`

## Notes

- The DAG schedules are environment-driven.
- Airflow is used for operational checks and table maintenance, not for long-running stream ownership.
