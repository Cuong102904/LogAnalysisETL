# Spark Repository

Spark ETL da duoc refactor theo clean architecture cho medallion lakehouse.

## Cac nhom thu muc chinh

- `apps/`: app entrypoints (`bronze_ingestor`, `silver_transformer`, `gold_aggregator`, `maintenance`).
- `domain/`: schemas va logic Bronze/Silver/Gold.
- `infrastructure/`: Spark/Kafka/Delta adapters.
- `configs/`: app config, rules, schemas, storage.
- `utils/`: helper dung chung.
- `tests/`: unit/integration fixtures.
- `conf/`: Spark runtime defaults.
- `jars/`: local jar cache.

## Trang thai implementation

- REAL: Bronze ingest, Silver normalize/classify, Gold video anomaly features.
- SKELETON: Gold profile tables (`user_learning_profile`, `problem_performance`, `system_profile`).
- Runtime: final local stack runs these apps with `spark-submit` against Spark Standalone.
