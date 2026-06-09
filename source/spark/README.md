# Spark Repository

Spark ETL da duoc refactor theo clean architecture cho medallion lakehouse.

## Cac nhom thu muc chinh

- `apps/`: runtime entrypoints (`bronze_ingestor`, `silver_transformer`, `gold_aggregator`, `gold_alerting`, `maintenance`).
- `pipelines/`: orchestration for read-transform-write and materialization flows.
- `domain/`: business logic for Bronze/Silver/Gold.
- `schemas/`: canonical raw/bronze/silver/gold table contracts.
- `infrastructure/`: Spark/Kafka/Delta adapters.
- `shared/`: generic helpers for config loading, JSON, logging, and UDFs.
- `configs/`: app config, rules, schemas, storage.
- `tests/`: unit/integration fixtures.
- `conf/`: Spark runtime defaults.
- `jars/`: local jar cache.

## Trang thai implementation

- REAL: Bronze ingest, Silver normalize/classify, Gold analytics tables (`video_friction_signals`, `pdf_engagement_features`, `quiz_attempt_metrics`, `user_learning_profile_daily`) plus anomaly signal tables (`exam_integrity_signals`, `behavior_anomaly_signals`) and alert materialization (`anomaly_alerts`).
- Runtime: final local stack runs these apps with `spark-submit` against Spark Standalone in near real-time micro-batch mode.
