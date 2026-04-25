# Airflow Repository

Airflow la orchestration layer cho phase tiep theo.

## Scope hien tai

- Giu skeleton folder:
  - `dags/`
  - `tasks/`
  - `plugins/`
  - `utils/`
- Chua implement runtime operators cho Bronze/Silver/Gold.

## Future DAG roadmap

- Trigger `apps.bronze_ingestor`.
- Trigger `apps.silver_transformer`.
- Trigger `apps.gold_aggregator`.
- Data quality checks va SLA alerts.

Trang thai: DEFERRED runtime, docs-first.

