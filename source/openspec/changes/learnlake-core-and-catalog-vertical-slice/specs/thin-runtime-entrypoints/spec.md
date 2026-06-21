## ADDED Requirements

### Requirement: Thin Spark entrypoints
The system SHALL provide thin Spark entrypoints for `run_bronze.py`, `run_silver.py`, and `run_gold.py` that parse arguments, load catalog profiles, initialize runtime objects, and call `learnlake` APIs.

#### Scenario: Silver entrypoint delegates to learnlake
- **WHEN** `apps/spark/run_silver.py --source daotao_ai` runs
- **THEN** the entrypoint loads the SourceProfile and delegates normalization to `learnlake` without implementing daotao-specific branches

### Requirement: Replay entrypoint remains application logic
The system SHALL provide a replay entrypoint that reads static daotao.ai fixtures or files and publishes raw events to Kafka using source event time for pacing, without writing Bronze tables.

#### Scenario: Replay publishes raw events only
- **WHEN** `apps/replay/replay_to_kafka.py --source daotao_ai` runs
- **THEN** it publishes raw events to the configured topic and leaves Bronze writing to the ingestion app

### Requirement: Broad runtime migration deferred
The system SHALL NOT require moving Airflow, Docker Compose, Trino, Superset, MinIO, or Hive metastore folders to complete this vertical slice.

#### Scenario: Vertical slice completes before platform migration
- **WHEN** the vertical slice tests pass
- **THEN** the framework boundary is considered proven even if broad platform layout migration remains for a later OpenSpec change
