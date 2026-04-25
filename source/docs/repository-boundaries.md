# Repository Boundaries

Muc tieu hien tai la clean architecture cho ETL Kafka -> Spark -> MinIO Delta.

## Repositories

- `infra-central`
  - Quan ly compose stack local, include Kafka/MinIO/Spark runtime wiring.
  - Giu khung `deploy/k8s` cho phase sau.
- `kafka`
  - Data ingress contracts va replay producer doc tu tracking logs thuc.
  - Topic canonical: `mooc.raw.events`, `mooc.dlq.events`.
- `spark`
  - ETL medallion theo clean layers: `apps/`, `domain/`, `infrastructure/`, `configs/`, `utils/`.
- `minio`
  - Bucket layout, scripts bootstrap, policy placeholders.
- `docs`
  - Design, runbook, schema/dedup/rules, troubleshooting.
- `airflow`
  - Skeleton orchestration, deferred runtime.
- `trino`
  - Skeleton query service, deferred runtime.

## Runtime Contract

- Canonical input topic: `mooc.raw.events`.
- DLQ topic: `mooc.dlq.events`.
- Bronze fanout topics la optional va mac dinh tat.
- Service DNS trong compose: `broker1:29092,broker2:29092,broker3:29092`.
