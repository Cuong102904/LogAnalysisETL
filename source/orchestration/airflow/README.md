# Airflow Repository

Airflow is the orchestration layer for operational checks and Delta maintenance across the lakehouse stack.

## What is implemented

- Runtime compose stack is provided by `source/docker-compose.yaml`.
- Reusable Spark wrapper task lives in `tasks/spark_task.py`.
- Bronze runtime checks live in `tasks/bronze/runtime_checks.py`.
- Airflow variable adapter lives in `config/variables.py`.

## Current DAGs

- Bronze
  - `bronze_stream_health`: Kafka readiness, Bronze checkpoint progress, and file health.
  - `bronze_table_maintenance`: Delta `OPTIMIZE` and optional `VACUUM` on the Bronze table.
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

## Project structure

- `dags/bronze/`, `dags/silver/`, `dags/gold/`: one DAG per file by domain.
- `tasks/`: reusable execution task factories (`spark_task`) and domain checks.
- `utils/`: shared helper utilities.
- `config/`: shared runtime variables loaded from Airflow Variables.

## Notes

- The DAG schedules are environment-driven.
- Airflow is used for operational checks and table maintenance, not for long-running stream ownership.










  Note: CHapter 