# Spark Repository

Spark ETL da duoc refactor theo clean architecture cho medallion lakehouse.

## Cac nhom thu muc chinh

- `apps/`: app entrypoints (`bronze_ingestor`, `silver_transformer`, `gold_aggregator`).
- `domain/`: schemas va logic Bronze/Silver/Gold.
- `infrastructure/`: Spark/Kafka/Delta adapters.
- `configs/`: app config, rules, schemas, storage.
- `utils/`: helper dung chung.
- `tests/`: unit/integration fixtures.
- `src/jobs/`: thin compatibility wrappers cho path cu.

## Trang thai implementation

- REAL: Bronze ingest, Silver normalize/classify, Gold video anomaly features.
- SKELETON: Gold profile tables (`user_learning_profile`, `problem_performance`, `system_profile`).
- DEFERRED: orchestration scheduling va query service o ngoai spark repo.

