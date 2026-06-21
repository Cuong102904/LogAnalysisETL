## 1. Package Skeleton and Contracts

- [x] 1.1 Update `pyproject.toml` for `src` package discovery and ensure `learnlake` is importable.
- [x] 1.2 Create minimal `src/learnlake/` package folders for `contracts`, `runtime`, `connectors`, `ingestion`, `normalization`, `quality`, `metrics`, and `plugins`.
- [x] 1.3 Define `BronzeEnvelope` contract with the required fields from the design.
- [x] 1.4 Define `LearningEvent` contract with required and optional canonical Silver fields from the design.
- [x] 1.5 Define `SourceProfile`, `MappingSpec`, quality rule, and metric execution profile contract models.

## 2. Runtime Helpers for Vertical Slice

- [x] 2.1 Implement minimal YAML config loading needed for catalog files.
- [x] 2.2 Implement minimal Spark session helper needed by vertical-slice apps.
- [x] 2.3 Implement minimal Delta read/write helpers for `bronze_events`, `silver_learning_events`, invalid events, and `gold_course_activity_summary`.
- [x] 2.4 Implement minimal Kafka/file input abstractions needed by Bronze ingestion and fixture-based tests.

## 3. Daotao Catalog and Fixtures

- [x] 3.1 Create `catalog/sources/daotao_ai.yaml` with `source_id: daotao_ai`, `source_type: edx_tracking_log`, input, Bronze, Silver, quality, and metric references.
- [x] 3.2 Create `catalog/mappings/daotao_ai_to_learning_event.yaml` using only `path`, `const`, `coalesce`, `cast`, `resolver`, and approved `plugin` operations.
- [x] 3.3 Create `catalog/event_types/edx_tracking_log.yaml` for daotao.ai event type to action/object/category/relevance mapping.
- [x] 3.4 Create `catalog/quality/common_learning_event_rules.yaml`.
- [x] 3.5 Create `catalog/quality/daotao_ai_rules.yaml`.
- [x] 3.6 Create `catalog/metrics/gold_course_activity_summary.yaml` with execution metadata and resource hints.
- [x] 3.7 Add daotao.ai fixture records outside `src/learnlake/` for Bronze, Silver, quality, and Gold tests.

## 4. Bronze Ingestion

- [x] 4.1 Implement Bronze envelope construction using configured source event-time fields as source of truth.
- [x] 4.2 Implement `bronze_events` writer partitioned by `source_id` and `processing_date`.
- [x] 4.3 Preserve raw payload and optional Kafka topic, partition, and offset metadata.
- [x] 4.4 Add Bronze fixture test coverage for out-of-order event times.

## 5. Mapping and Silver Normalization

- [x] 5.1 Implement MappingSpec parser and validator.
- [x] 5.2 Implement `path`, `const`, `coalesce`, and `cast` mapping operations.
- [x] 5.3 Implement catalog-backed event type resolver.
- [x] 5.4 Implement internal transform registry and reject unregistered plugin names.
- [x] 5.5 Register approved daotao transform plugins outside framework core where source-specific extraction is required.
- [x] 5.6 Implement normalization from `bronze_events` to canonical `silver_learning_events`.
- [x] 5.7 Ensure normalization does not rely on ingest order, arrival order, or file line order.

## 6. Quality Validation

- [x] 6.1 Implement common quality rule parsing and validation.
- [x] 6.2 Implement daotao.ai-specific quality rule loading from catalog.
- [x] 6.3 Populate `quality_status` and `quality_errors` on canonical Silver records.
- [x] 6.4 Write invalid or ignored records to a traceable invalid/quarantine output with `raw_event_ref`.

## 7. Gold Metric Vertical Slice

- [x] 7.1 Implement metric definition loading for `gold_course_activity_summary`.
- [x] 7.2 Implement metric execution profile parsing for mode, priority, trigger interval, enabled state, and resource hints.
- [x] 7.3 Implement `gold_course_activity_summary` from `silver_learning_events`.
- [x] 7.4 Verify the metric does not parse Bronze raw payloads or daotao.ai raw schema directly.

## 8. Thin Entry Points

- [x] 8.1 Create `apps/spark/run_bronze.py` as a thin source-profile-driven Bronze entrypoint.
- [x] 8.2 Create `apps/spark/run_silver.py` as a thin source-profile-driven Silver entrypoint.
- [x] 8.3 Create `apps/spark/run_gold.py` as a thin source-profile and metric-profile-driven Gold entrypoint.
- [x] 8.4 Create or adapt `apps/replay/replay_to_kafka.py` for daotao.ai raw event replay without Bronze writing.
- [x] 8.5 Keep broad Airflow, Docker Compose, Trino, Superset, MinIO, and Hive metastore layout migration out of this change.

## 9. Tests and Acceptance Criteria

- [x] 9.1 Add contract tests for `BronzeEnvelope`, `LearningEvent`, `SourceProfile`, `MappingSpec`, quality rule, and metric execution profile validation.
- [x] 9.2 Add unit tests for mapping operations and event type resolver behavior.
- [x] 9.3 Add unit tests for plugin registry approval and rejection behavior.
- [x] 9.4 Add quality validation tests for valid, warning, invalid, ignored, bot/noise, and unknown event cases.
- [x] 9.5 Add integration test AC1: daotao.ai fixture events produce `bronze_events` with raw payload and source metadata.
- [x] 9.6 Add integration test AC2: daotao.ai Bronze records produce valid `silver_learning_events` required fields.
- [x] 9.7 Add integration test AC3: `gold_course_activity_summary` is produced from Silver without raw payload parsing.
- [x] 9.8 Add acceptance test AC4: changing daotao.ai event type mapping does not require modifying `src/learnlake/`.
- [x] 9.9 Add static check AC5 for forbidden daotao.ai or edX-specific identifiers under `src/learnlake/`.
- [x] 9.10 Run `uv run pytest` for all affected contract, unit, and integration tests.
