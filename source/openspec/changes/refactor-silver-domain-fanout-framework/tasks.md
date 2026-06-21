## 1. Contract And Profile Model

- [x] 1.1 Add `EventIndex`, `FactRecord`, `NormalizationResult`, and invalid-output contracts under `src/learnlake/contracts`.
- [x] 1.2 Add domain fact contracts or schema descriptors for assessment, video, document, navigation, exam, course content, authoring, auth, system, and unknown Silver tables.
- [x] 1.3 Replace or supersede the current `LearningEvent` contract with `EventIndex`, keeping a temporary compatibility alias only if existing tests require it.
- [x] 1.4 Extend `SourceProfile.silver` to support `event_index`, `targets`, `invalid`, routing references, mapping references, table names, paths, checkpoints, schemas, partitions, and quality rule references.
- [x] 1.5 Update source profile validation so enabled Silver targets require valid table names and paths before runtime starts.
- [x] 1.6 Update `catalog/sources/daotao_ai.yaml` to declare `silver_event_index`, domain fact targets, and `silver_invalid_events` instead of a single `silver_learning_events` target.

## 2. Routing And Extraction Engine

- [x] 2.1 Add route model contracts for route id, priority, match expression, event group, normalized type, action, object type, target tables, and extractor name.
- [x] 2.2 Implement constrained route operators: equality, membership, prefix, substring, regex, all/any/not composition, event source checks, host checks, context key checks, and parsed payload key checks.
- [x] 2.3 Add route file loading and validation that rejects unsupported operators and malformed target/extractor declarations.
- [x] 2.4 Implement deterministic first-match routing using explicit priority and stable tie-breaking.
- [x] 2.5 Add parsed payload normalization for raw `event` values represented as JSON strings, dicts, lists, URL/form-encoded strings, and invalid payloads.
- [x] 2.6 Extend the approved plugin registry or add an extractor registry that invokes only registered extractor names.
- [x] 2.7 Implement the generic normalizer flow that builds an event-index draft, resolves a route, invokes extractors, validates outputs, and returns `NormalizationResult`.

## 3. Daotao Source Pack

- [x] 3.1 Create daotao.ai routing declarations for assessment, video, document/PDF, navigation/UI, exam, course content, authoring/studio, auth/account, system/noise, and unknown fallback events.
- [x] 3.2 Encode priority rules so assessment endpoints beat generic course routes, PDF routes beat generic course routes, proctoring routes map to exam, and bot/scanner routes map to system/noise.
- [x] 3.3 Move Open edX event-name semantics such as `problem_check`, `problem_graded`, `edx.grades.problem.submitted`, `play_video`, and `textbook.pdf.*` out of framework core and into the daotao source pack.
- [x] 3.4 Implement daotao common extraction utilities for Open edX usage keys, block type, block id, course id, path, page, request GET/POST payloads, and sensitive auth redaction.
- [x] 3.5 Register daotao source-pack transforms and extractors during `daotao_ai` runtime initialization.
- [x] 3.6 Update `projects/daotao_ai/mapping_notes.md` and `projects/daotao_ai/data_dictionary.md` to document event groups, normalized types, and extraction assumptions.

## 4. Domain Fact Extractors

- [x] 4.1 Implement daotao assessment extractors for browser submit, server submit endpoint, server check, grade persisted, graded HTML metadata, input AJAX, and show-answer events.
- [x] 4.2 Implement daotao video extractors for browser load/play/pause/stop/seek/speed-change, server save-position, transcript, and completion events.
- [x] 4.3 Implement daotao document/PDF extractors for PDF book page load, `book`, zoom/scale, page scroll, chapter navigation, search execution, and search navigation events.
- [x] 4.4 Implement daotao navigation/UI extractors for link clicks, sequence next/previous/tab events, course resume, and display/upsell events.
- [x] 4.5 Implement daotao exam extractors for timed exam lifecycle, proctoring API events, and quiz navigation xblock endpoints.
- [x] 4.6 Implement daotao course content extractors for course home, courseware page, jump-to, progress, about, cohorts, and completion endpoints.
- [x] 4.7 Implement daotao authoring/studio extractors for library pages, xblock preview, container preview, author view, studio view, refresh children, and grading policy changes.
- [x] 4.8 Implement daotao auth/account and system/noise extractors, ensuring OAuth codes/tokens are not copied into Silver outputs.
- [x] 4.9 Implement unknown-event extraction that retains event-index lineage and records unmatched raw event identity for follow-up mapping.

## 5. Silver Runtime Fan-Out

- [x] 5.1 Update `apps/spark/run_silver.py` batch mode to call the multi-target normalizer and group `FactRecord` outputs by target table.
- [x] 5.2 Update `apps/spark/run_silver.py` streaming mode to write `silver_event_index`, each configured domain target, and invalid output in micro-batches.
- [x] 5.3 Replace the fixed `SILVER_SCHEMA` with per-target schema handling for event index and domain fact tables.
- [x] 5.4 Ensure empty target groups are skipped without failing the batch or stream.
- [x] 5.5 Ensure per-target checkpoints are resolved from the source profile and do not reuse the same checkpoint across independent Delta sinks.
- [x] 5.6 Update local environment overrides in `apps/spark/common.py` and Docker Compose variables so Silver paths/checkpoints can be configured per target.
- [x] 5.7 Keep or add a temporary compatibility view/path for `silver_learning_events` only if existing POC assets require a migration bridge.

## 6. Quality, Validation, And Tests

- [x] 6.1 Add unit tests for route matching operators, route priority, unsupported operator rejection, and unregistered extractor rejection.
- [x] 6.2 Add unit tests for parsed payload normalization across JSON string, dict, list, form-encoded string, and invalid payload cases.
- [x] 6.3 Add contract tests proving files under `src/learnlake/` contain no daotao.ai/Open edX routing identifiers or fixed daotao runtime paths.
- [x] 6.4 Add fixture tests proving daotao `play_video` produces one event-index row and one video fact row.
- [x] 6.5 Add fixture tests proving daotao assessment submit/check/grade events populate event-index and assessment fact rows with expected typed fields.
- [x] 6.6 Add fixture tests proving daotao PDF/book events populate document fact rows with expected scroll, zoom, chapter, search, or page fields.
- [x] 6.7 Add fixture tests proving ambiguous routes such as `/xblock/.../problem_check`, `/pdfbook/`, and `/api/edx_proctoring/` route to the intended domain.
- [x] 6.8 Add quality tests proving invalid required event-index fields route to invalid output and do not emit domain fact rows.
- [x] 6.9 Add tests proving source event `time` drives `event_time` and ingest/file order is not used for sequencing.
- [x] 6.10 Update or replace existing tests that assume the only Silver output is `silver_learning_events`.

## 7. Documentation And Downstream Compatibility

- [x] 7.1 Update `docs/schema-strategy.md` or add a Silver model document describing event index plus domain fact tables.
- [x] 7.2 Update `docs/medallion.md` to explain why Silver contains both a common event index and typed domain facts.
- [x] 7.3 Update `docs/event-flow.md` to show Bronze to multi-target Silver flow and invalid/unknown routing.
- [x] 7.4 Update `projects/daotao_ai/README.md` with the daotao source-pack role and examples of mapped Open edX events.
- [x] 7.5 Review Gold builders, Trino views, notebooks, and Superset bootstrap files for dependencies on the old single Silver table and either update them or document deferred migration.

## 8. Verification

- [x] 8.1 Run unit and contract tests with `uv run pytest tests spark/tests`.
- [x] 8.2 Run a fixture-based Bronze-to-Silver normalization smoke test for daotao.ai in batch mode and verify `silver_event_index` plus at least assessment, video, document, and exam target outputs.
- [x] 8.3 Run static checks that source-specific routing names are absent from `src/learnlake/` and present only in source-pack/catalog files.
- [x] 8.4 Start the requested local runtime subset with `docker compose up -d broker1 broker2 broker3 kafka-ui kafka-init minio minio-init bronze-stream silver-stream tracking-log-replayer spark-master spark-worker-1`.
- [x] 8.5 Inspect Docker Compose service status and logs for broker, MinIO, Spark master/worker, tracking-log replayer, Bronze stream, and Silver stream readiness or actionable failures.
- [x] 8.6 Verify that replayed daotao.ai events reach Kafka, Bronze writes to MinIO/Delta, and Silver writes `silver_event_index` plus at least one relevant domain fact target.
- [x] 8.7 Document any Docker Compose verification failure with the failing service, command output summary, suspected cause, and exact next action.
