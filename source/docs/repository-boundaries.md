# Repository Boundaries

Muc tieu hien tai la clean architecture cho ETL Kafka -> Spark Bronze -> MinIO Delta.

## Repositories

- `kafka`
  - Data ingress contracts va replay producer doc tu tracking logs thuc.
  - Topic canonical: `mooc.raw.events`, `mooc.dlq.events`.
- `spark`
  - ETL Bronze theo clean layers: `apps/`, `domain/`, `infrastructure/`, `configs/`, `utils/`.
- `minio`
  - Bucket layout, scripts bootstrap, policy placeholders.
- `docs`
  - Design, runbook, schema/dedup/rules, troubleshooting.
- `airflow`
  - Scheduling and maintenance for Bronze only.

## Runtime Contract

- Canonical input topic: `mooc.raw.events`.
- DLQ topic: `mooc.dlq.events`.
- Service DNS trong compose: `broker1:29092,broker2:29092,broker3:29092`.
