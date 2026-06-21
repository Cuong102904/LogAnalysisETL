## 1. Package and Repository Skeleton

- [ ] 1.1 Update `pyproject.toml` for a `src` package layout and ensure `learnlake` is importable through `src/learnlake`.
- [ ] 1.2 Create `src/learnlake/` package roots for `contracts`, `connectors`, `ingestion`, `normalization`, `quality`, `metrics`, `runtime`, and optional CLI modules.
- [ ] 1.3 Create top-level target folders `catalog/`, `apps/`, `projects/`, `platform/local/`, `orchestration/`, `serving/`, and `tests/`.
- [ ] 1.4 Add package `__init__.py` files and minimal public API exports for the framework modules.

## 2. Core Contracts and Runtime Helpers

- [ ] 2.1 Define Bronze event envelope contract with source identity, source event type, event-time raw value, ingestion time, raw payload, schema version, and Kafka metadata fields.
- [ ] 2.2 Define canonical `LearningEvent` contract for `silver_learning_events`.
- [ ] 2.3 Move generic config loading into `src/learnlake/runtime/config.py`.
- [ ] 2.4 Move Spark session creation into `src/learnlake/runtime/spark.py`.
- [ ] 2.5 Move generic logging and runtime error helpers into `src/learnlake/runtime/`.
- [ ] 2.6 Move Delta storage helpers and table path handling into `src/learnlake/connectors/delta.py` or `src/learnlake/connectors/`.
- [ ] 2.7 Move Kafka and file connector abstractions into `src/learnlake/connectors/`.

## 3. Source Profile and Catalog

- [ ] 3.1 Define a SourceProfile loader and validation model for `catalog/sources/<source_id>.yaml`.
- [ ] 3.2 Create `catalog/sources/daotao_ai.yaml` with `source_id: daotao_ai` and `source_type: edx_tracking_log`.
- [ ] 3.3 Create `catalog/mappings/daotao_ai_to_learning_event.yaml` for canonical `LearningEvent` fields.
- [ ] 3.4 Create `catalog/event_types/edx_tracking_log.yaml` by migrating edX/MOOC event classification rules out of Spark domain code.
- [ ] 3.5 Create `catalog/quality/common_learning_event_rules.yaml` for required canonical Silver fields.
- [ ] 3.6 Create `catalog/quality/daotao_ai_rules.yaml` for daotao.ai-specific validation rules.
- [ ] 3.7 Create initial metric definitions under `catalog/metrics/` for the first Gold vertical slice.

## 4. Ingestion and Normalization Engines

- [ ] 4.1 Implement Bronze envelope construction in `src/learnlake/ingestion/envelope.py`.
- [ ] 4.2 Implement Bronze writer orchestration in `src/learnlake/ingestion/bronze_writer.py`.
- [ ] 4.3 Implement mapping file parsing and field resolution in `src/learnlake/normalization/mapper.py`.
- [ ] 4.4 Implement supported mapping operations for field path, constant, expression, resolver, type conversion, and raw event reference.
- [ ] 4.5 Implement event type resolver that reads `catalog/event_types/*.yaml`.
- [ ] 4.6 Implement timestamp normalization using source profile event-time fields as source of truth.
- [ ] 4.7 Implement deduplication support without relying on ingest order or file line order.
- [ ] 4.8 Implement Silver normalizer runner that writes canonical `silver_learning_events`.
- [ ] 4.9 Add optional plugin hook loading for source-specific transforms outside `src/learnlake/`.

## 5. Quality and Metrics

- [ ] 5.1 Implement quality rule parsing in `src/learnlake/quality/rules.py`.
- [ ] 5.2 Implement quality validation in `src/learnlake/quality/validator.py`.
- [ ] 5.3 Implement invalid record reporting or quarantine behavior in `src/learnlake/quality/quarantine.py`.
- [ ] 5.4 Move or rewrite generic Gold builders so they read canonical Silver data instead of raw source payloads.
- [ ] 5.5 Implement at least one daotao.ai metric vertical slice using `catalog/metrics/` and `learnlake.metrics`.

## 6. Executable Apps and Replay

- [ ] 6.1 Create `apps/spark/run_bronze.py` as a thin entrypoint that loads a source profile and calls `learnlake.ingestion`.
- [ ] 6.2 Create `apps/spark/run_silver.py` as a thin entrypoint that loads a source profile and calls `learnlake.normalization`.
- [ ] 6.3 Create `apps/spark/run_gold.py` as a thin entrypoint that loads a source profile and metric definition and calls `learnlake.metrics`.
- [ ] 6.4 Move Kafka replay pacing and event-clock logic into `apps/replay/`.
- [ ] 6.5 Move daotao.ai static-log replay adapter into `apps/replay/sources/daotao_ai.py` or a project plugin outside framework core.
- [ ] 6.6 Create `apps/replay/replay_to_kafka.py` as a thin replay application that publishes static events to Kafka by source event time.
- [ ] 6.7 Create bootstrap entrypoints under `apps/bootstrap/` for topics, buckets, and table registration as needed.

## 7. Project, Platform, Orchestration, and Serving Layout

- [ ] 7.1 Create `projects/daotao_ai/README.md`, data dictionary, mapping notes, and sample data structure.
- [ ] 7.2 Move Airflow DAGs and task wrappers into `orchestration/airflow/`.
- [ ] 7.3 Update Airflow tasks to invoke thin app entrypoints instead of old Spark paths.
- [ ] 7.4 Move local platform assets for Kafka, Spark, MinIO, Hive metastore, Trino, and Superset into `platform/local/`.
- [ ] 7.5 Move Trino and Superset serving assets into `serving/`.
- [ ] 7.6 Update Docker Compose, Dockerfiles, environment variables, and mounted paths for the new layout.
- [ ] 7.7 Update documentation and runbooks to describe framework core, catalog, apps, projects, platform, orchestration, and serving boundaries.

## 8. Tests and Validation

- [ ] 8.1 Add contract tests for source profile schema validation.
- [ ] 8.2 Add contract tests for mapping schema validation.
- [ ] 8.3 Add contract tests for required `LearningEvent` fields.
- [ ] 8.4 Add unit tests for event type resolver behavior using `catalog/event_types/edx_tracking_log.yaml`.
- [ ] 8.5 Add unit tests for quality rule parsing and validation behavior.
- [ ] 8.6 Add integration test for daotao.ai raw fixture to canonical `LearningEvent` normalization.
- [ ] 8.7 Add test or static check that `src/learnlake/` does not contain daotao.ai or edX-specific identifiers.
- [ ] 8.8 Run `uv run pytest` for affected tests.
- [ ] 8.9 Run documented Docker/Airflow/Spark smoke checks for the daotao.ai vertical slice.

## 9. Cleanup and Compatibility Removal

- [ ] 9.1 Remove old `spark/` modules after their reusable logic has migrated or been replaced.
- [ ] 9.2 Remove old `kafka/src/` replay modules after replay has migrated to `apps/replay/`.
- [ ] 9.3 Remove or relocate old top-level runtime folders that have moved to `platform/local/`, `orchestration/`, or `serving/`.
- [ ] 9.4 Remove stale imports, path hacks, compatibility wrappers, and references to old runtime entrypoints.
- [ ] 9.5 Verify the repository no longer treats Spark, Kafka, Airflow, MinIO, Trino, or Superset folders as owners of reusable framework logic.
