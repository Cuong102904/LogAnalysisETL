# Log Streaming Platform

This repository runs a local Kafka -> Spark Bronze -> MinIO Delta -> Airflow stack.

## Folder Layout

Root files:

- `docker-compose.yaml`: single compose entrypoint for the whole stack.
- `.env`: required local runtime values loaded automatically by Docker Compose.
- `.env.example`: template for the required values.
- `README.md`: this file.
- `docs/`: runbook, architecture notes, troubleshooting, and design decisions.
- `deploy/scripts/`: operational helpers such as the Spark smoke test.

Service folders:

- `kafka/`: raw event ingress, replay producer, topic bootstrap, and Kafka image build.
- `spark/`: Bronze streaming job, maintenance job, shared domain logic, and Spark runtime config.
- `minio/`: bucket bootstrap and storage bootstrap helpers.
- `airflow/`: DAGs, task wrappers, and shared runtime helpers for Bronze and Silver maintenance.

## Best-Practice Structure By Service

`kafka/`
- `src/`: Python package for producers, filters, adapters, and shared helpers.
- `config/`: topic and producer config.
- `schemas/`: data contracts.
- `scripts/`: container bootstrap scripts.
- `tests/`: unit and integration tests.

`spark/`
- `apps/`: executable Spark entrypoints such as `bronze_ingestor`, `silver_transformer`, and maintenance jobs.
- `domain/`: pure business logic and schemas.
- `infrastructure/`: Spark, Kafka, and storage adapters.
- `configs/`: app, schema, rule, and storage YAML files.
- `conf/`: Spark runtime defaults.
- `tests/`: unit and integration tests.

`airflow/`
- `dags/`: scheduled workflows.
- `tasks/`: reusable task helpers and checks.
- `config/`: shared runtime variables.
- `utils/`: small shared utilities.
- `plugins/`: extension point for future Airflow plugins.

`minio/`
- `bootstrap/`: bucket layout notes.
- `scripts/`: bootstrap scripts for local setup.
- `policies/`: policy placeholders.

## Environment Model

There is one required env file only: `source/.env`.

Docker Compose loads `.env` automatically when you run commands from `source/`, so you do not need `--env-file`.

Create it once from the template:

```bash
cp .env.example .env
```

Keep service-specific variables grouped by prefix:

- `KAFKA_*`
- `MINIO_*`
- `SPARK_*`
- `AIRFLOW_*`
- `BRONZE_*`
- `SILVER_*`

## Run

From `source/`:

```bash
docker compose up -d
```

If you just changed a Dockerfile or want to rebuild images, use:

```bash
docker compose up -d --build
```

## UIs

- Kafka UI: `http://localhost:8085`
- Spark Master: `http://localhost:8081`
- Spark Worker 1: `http://localhost:8082`
- Spark History Server: `http://localhost:18080`
- Airflow: `http://localhost:8089`
- MinIO: `http://localhost:9001`

## Notes

- The current runtime scope includes Kafka ingest, Spark Bronze stream, Spark Silver stream, and Airflow maintenance.
- Gold remains in the codebase and can be explored, but it is not part of the continuously running local stack.
