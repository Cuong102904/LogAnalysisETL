## Why

The repository is currently organized around runtime technologies (`spark`, `kafka`, `airflow`, `trino`, `superset`) rather than around a reusable learning-log analytics framework. This makes the current daotao.ai/edX logic look like the framework itself, and it leaves no clear contract for adding another source such as EdNet without changing core pipeline code.

## What Changes

- **BREAKING**: Replace the technology-first repository layout with a framework-first layout centered on `src/learnlake/`, `catalog/`, `apps/`, `projects/`, `platform/local/`, `orchestration/`, `serving/`, and `tests/`.
- **BREAKING**: Move reusable Spark/Kafka/Delta/config logic into a Python package under `src/learnlake/` and make runtime entrypoints call the package through stable framework APIs.
- **BREAKING**: Treat daotao.ai as the first concrete source profile and case study, not as framework core.
- Introduce a canonical `LearningEvent` Silver contract and a common Bronze event envelope used by all source profiles.
- Introduce declarative source profiles, field mappings, event type mappings, quality rules, and metric definitions under `catalog/`.
- Introduce source-specific project space under `projects/daotao_ai/` for documentation, data dictionary, mapping notes, samples, and optional source-specific transform plugins.
- Move static dataset replay into `apps/replay/` as an application that simulates event-time streaming into Kafka.
- Keep Spark, Kafka, Airflow, MinIO, Hive metastore, Trino, and Superset as runtime/platform concerns rather than places that own reusable domain logic.
- Add contract tests proving that a source profile can normalize raw events into the canonical `LearningEvent` model without modifying `src/learnlake/`.

## Capabilities

### New Capabilities

- `learnlake-core-framework`: Defines the source-agnostic framework package, public APIs, contracts, ingestion engine, normalization engine, quality engine, generic metric builders, connectors, and runtime helpers.
- `source-profile-catalog`: Defines declarative source profiles, mappings, event type maps, quality rules, metric definitions, and extension boundaries for source-specific logic.
- `framework-runtime-layout`: Defines the executable apps, daotao.ai case study layout, local platform layout, orchestration layout, serving layout, and test organization that run and validate the framework.

### Modified Capabilities

- None.

## Impact

- Affected code: top-level repository structure, `spark/`, `kafka/`, `airflow/`, `trino/`, `superset/`, `minio/`, `hive-metastore/`, `deploy/`, Python imports, runtime entrypoints, Docker build context, tests, and documentation.
- Affected APIs: new public Python APIs under `learnlake.*` and optional CLI/entrypoint commands such as `learnlake ingest`, `learnlake normalize`, `apps/spark/run_bronze.py`, `apps/spark/run_silver.py`, and `apps/spark/run_gold.py`.
- Affected runtime integrations: Spark submit targets, Airflow DAG task commands, Docker Compose service paths, Kafka topic bootstrap scripts, MinIO bucket/table bootstrap scripts, Trino view paths, and Superset bootstrap paths.
- Affected data contracts: Bronze events become a common envelope, Silver centers on canonical `silver_learning_events`, and source-specific semantic projections become optional extensions.
- Affected developer workflow: adding a new source must be done through `catalog/`, `projects/`, fixtures, and optional plugins without requiring changes to `src/learnlake/`.
