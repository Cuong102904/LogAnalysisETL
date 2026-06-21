## Why

The current framework-oriented Silver path normalizes learning logs into a single generic `silver_learning_events` shape, which is useful for interoperability but not sufficient for analytics-ready Silver data. Learning logs need a source-agnostic event index plus typed domain fact tables so Gold jobs can read assessment, video, document, navigation, and exam data without reparsing raw JSON or embedding daotao.ai/Open edX assumptions in framework core.

## What Changes

- **BREAKING**: Reframe the canonical Silver output from a single `LearningEvent` table into a multi-target Silver model:
  - `silver_event_index` as the common event index/envelope for every normalized event.
  - Domain fact tables such as `silver_assessment_events`, `silver_video_events`, `silver_document_events`, `silver_navigation_events`, `silver_exam_events`, `silver_course_content_events`, `silver_authoring_events`, `silver_auth_events`, `silver_system_events`, and `silver_unknown_events`.
- Replace the one-target normalizer result with a `NormalizationResult` that can contain one event-index row plus zero or more domain fact rows.
- Move source-specific classification and field extraction into source packs, starting with `projects/daotao_ai`, instead of hard-coded Open edX/daotao.ai logic in `src/learnlake`.
- Add declarative routing rules that map raw source events to canonical event groups, normalized types, target Silver tables, and approved extractor functions.
- Keep `src/learnlake` source-agnostic: it loads source profiles, compiles routing/mapping declarations, invokes approved plugins, validates outputs, and writes configured targets.
- Preserve raw payloads and full lineage in Bronze and through `raw_event_ref`; Silver stores typed analysis fields and only selected payload JSON needed for traceability.
- Update runtime configuration, tests, and documentation so daotao.ai is the first source pack proving the framework model.
- Add an end-to-end verification task that can run the local Docker Compose subset:
  `docker compose up -d broker1 broker2 broker3 kafka-ui kafka-init minio minio-init bronze-stream silver-stream tracking-log-replayer spark-master spark-worker-1`.

## Capabilities

### New Capabilities

- `silver-event-index`: Defines the source-agnostic Silver event index contract used for lineage, common filtering, normalized event semantics, and joins to domain fact tables.
- `silver-domain-fact-tables`: Defines typed analytics-ready Silver fact tables for assessment, video, document/PDF, navigation, exam, course content, authoring, auth, system/noise, and unknown events.
- `source-pack-routing-extraction`: Defines how source packs declare routing rules and approved extractors outside framework core, with daotao.ai as the first implementation.
- `multi-target-silver-normalization`: Defines the normalization engine behavior for producing event-index rows, domain fact rows, invalid outputs, quality results, and per-target writes.

### Modified Capabilities

- None. Existing OpenSpec specs have not been archived into `openspec/specs/`; this change introduces replacement capabilities for the next Silver architecture rather than modifying archived requirements.

## Impact

- Affected framework modules:
  - `src/learnlake/contracts/learning_event.py` will be replaced or superseded by event index and fact contracts.
  - `src/learnlake/normalization/normalizer.py`, `mapper.py`, and related runtime APIs will produce multi-target results.
  - `src/learnlake/plugins/registry.py` remains the approved plugin boundary but must support source-pack extractor registration.
- Affected daotao.ai source pack:
  - `projects/daotao_ai/transforms.py` will grow into routing/extractor plugin registration.
  - `catalog/mappings/daotao_ai_to_learning_event.yaml` will be split or superseded by event-index and domain mappings.
  - New daotao.ai routing and extraction catalog files will define Open edX event mapping outside core.
- Affected Spark/runtime apps:
  - `apps/spark/run_silver.py` and/or `spark/pipelines/silver/transform_pipeline.py` must write multiple Silver targets from one Bronze input stream.
  - Existing legacy `spark/domain/silver/normalizers/*` can be used as migration reference but should not remain the framework source of truth.
- Affected downstream analytics:
  - Gold jobs and Trino/Superset views should read domain fact tables where possible instead of reparsing generic payload JSON.
  - Existing paths and table names may need migration aliases during the POC.
- Verification:
  - Unit/contract tests must prove framework core has no daotao.ai/Open edX hard-coding.
  - Integration tests must prove daotao.ai fixture events populate event-index and domain fact tables.
  - A final local Docker Compose smoke check must verify Bronze and Silver streams run with Kafka, MinIO, Spark, and the tracking-log replayer.
