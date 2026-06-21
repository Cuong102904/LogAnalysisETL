## Context

The existing repository already has a functioning Spark/Kafka lakehouse prototype, but the current code is organized by runtime technology and still mixes reusable pipeline mechanics with daotao.ai/edX-specific event semantics. The broad draft change `restructure-repo-as-learnlake-framework` correctly identifies the final direction, but its scope is too large for one safe implementation pass.

This change narrows the work to a proof-oriented vertical slice:

```text
daotao.ai fixture/raw stream
        |
        v
bronze_events
        |
        v
silver_learning_events
        |
        v
gold_course_activity_summary
```

The framework is source-extensible at the learning-log schema level, but the prototype remains runtime-bound to Spark Structured Streaming, Delta Lake, Kafka, and the existing local lakehouse stack.

## Goals / Non-Goals

**Goals:**

- Create a minimal `src/learnlake/` package boundary.
- Define concrete `BronzeEnvelope`, `LearningEvent`, `SourceProfile`, `MappingSpec`, quality rule, and metric execution profile contracts.
- Implement one common `bronze_events` table partitioned by `source_id` and `processing_date`.
- Implement canonical `silver_learning_events` as the required Silver semantic layer.
- Implement a constrained mapping engine for daotao.ai using catalog declarations.
- Move edX event type semantics into `catalog/event_types/edx_tracking_log.yaml`.
- Use an internal transform registry for approved source-specific plugins.
- Implement one Gold metric: `gold_course_activity_summary`.
- Add tests that prove daotao.ai can run through the framework boundary without hard-coded daotao.ai/edX rules in `src/learnlake/`.

**Non-Goals:**

- Full repository layout migration.
- Full Airflow, Docker Compose, Trino, Superset, MinIO, or Hive metastore path migration.
- A general-purpose CLI under `learnlake`.
- Full EdNet implementation.
- Engine-agnostic runtime support.
- A distributed scheduler or resource optimizer.
- Arbitrary Python imports from YAML.
- Arbitrary SQL/Python expressions in YAML.
- Video, exam, PDF, navigation, or system Silver projection tables unless required by the first metric.

## Decisions

### 1. Build the framework boundary before moving the whole repo

The first implementation will create `src/learnlake/`, catalog files, thin app entrypoints, and tests for the daotao.ai vertical slice. Broad runtime layout migration is deferred until the core boundary is proven.

Alternatives considered:

- Move all top-level folders now. Rejected because it risks breaking runtime integration before contracts and tests are reliable.
- Keep improving only `spark/`. Rejected because it does not establish a reusable framework boundary.

### 2. Use one Bronze table for the first implementation

The first implementation uses one `bronze_events` Delta table partitioned by `source_id` and `processing_date`.

Required Bronze fields:

| Field | Type | Required | Semantics |
| --- | --- | --- | --- |
| `event_id` | string | yes | Framework-generated Bronze event id |
| `source_id` | string | yes | Concrete source, e.g. `daotao_ai` |
| `source_type` | string | yes | Source format, e.g. `edx_tracking_log` |
| `source_event_type` | string/null | no | Raw source event type if available |
| `event_time_raw` | string/null | no | Raw event-time value from source payload |
| `event_time` | timestamp/null | no | Parsed event time when parseable |
| `ingestion_time` | timestamp | yes | Time the framework ingested the record |
| `raw_payload` | string/json | yes | Complete raw source payload |
| `kafka_topic` | string/null | no | Kafka topic when Kafka is input |
| `kafka_partition` | int/null | no | Kafka partition when Kafka is input |
| `kafka_offset` | long/null | no | Kafka offset when Kafka is input |
| `schema_version` | string | yes | Bronze envelope schema version |
| `processing_date` | date | yes | Date partition derived from ingestion time |

Per-source Bronze tables are a future extension only.

### 3. Define `LearningEvent` as the canonical Silver semantic layer

`silver_learning_events` is the required normalized semantic table. It is not the full-detail table; full raw detail remains in Bronze. Specialized video, exam, PDF, navigation, or system projection tables are deferred unless needed later.

Required `LearningEvent` fields:

| Field | Type | Required | Semantics |
| --- | --- | --- | --- |
| `event_id` | string | yes | Stable Silver event id |
| `source_id` | string | yes | Concrete source id |
| `source_type` | string | yes | Source format/type |
| `actor_id` | string/null | no | Normalized learner/user id when known |
| `actor_external_id` | string/null | no | Raw platform username or external id |
| `session_id` | string/null | no | Source session id when available |
| `course_id` | string/null | no | Course/context id when available |
| `org_id` | string/null | no | Organization id when available |
| `event_time` | timestamp | yes | Source event time, not ingest order |
| `raw_event_type` | string | yes | Raw source event type |
| `event_source` | string/null | no | Raw source subsystem, e.g. browser/server |
| `action` | string | yes | Normalized action |
| `object_type` | string | yes | Normalized object type |
| `object_id` | string/null | no | Normalized object id when extractable |
| `event_category` | string | yes | Broad category such as learning, assessment, navigation, system, noise, unknown |
| `learning_relevance` | string | yes | `learning`, `non_learning`, `noise`, or `unknown` |
| `is_authenticated` | boolean | yes | Whether actor identity is present |
| `is_bot` | boolean | yes | Whether event is detected as bot/crawler/noise |
| `quality_status` | string | yes | `valid`, `warning`, `invalid`, or `ignored` |
| `quality_errors` | array<string> | yes | Rule ids or messages for quality issues |
| `raw_event_ref` | string | yes | Reference to Bronze `event_id` |
| `context` | map/json | no | Normalized context payload |
| `processing_time` | timestamp | yes | Time the framework produced Silver output |

### 4. Keep mapping v1 deliberately constrained

The first mapping engine supports only:

- `path`: read a value from raw payload or Bronze field.
- `const`: assign a fixed value.
- `coalesce`: choose the first non-null value from allowed mappings.
- `cast`: cast to simple types such as string, int, boolean, timestamp.
- `resolver`: map an input through a catalog resolver such as event type mapping.
- `plugin`: call an approved transform from an internal registry.

It does not support arbitrary expressions such as `sha2(concat(...))` or arbitrary Python/SQL code in YAML.

### 5. Use an internal plugin registry

YAML mapping files may reference plugin names, not import paths. Approved transforms are registered in Python by name.

```yaml
object_id:
  plugin:
    name: daotao_extract_object_id
    inputs:
      - path: raw_payload.context.path
      - path: raw_payload.event_type
```

This prevents arbitrary imports from YAML while still allowing source-specific transforms outside `src/learnlake/`.

### 6. Use a lightweight analysis execution profile

Metric definitions may include execution metadata such as mode, priority, trigger interval, enabled flag, and resource hints. This change does not implement scheduling optimization. Spark and Airflow remain responsible for execution; the profile only declares intent.

### 7. First Gold metric is `gold_course_activity_summary`

The first Gold metric reads `silver_learning_events` and writes `gold_course_activity_summary`.

Fields:

| Field | Semantics |
| --- | --- |
| `source_id` | Source id |
| `course_id` | Course id or null bucket |
| `window_start` | Metric window start |
| `window_end` | Metric window end |
| `active_learners` | Distinct authenticated non-bot learners |
| `learning_event_count` | Count of learning events |
| `noise_event_count` | Count of noise events |
| `unknown_event_count` | Count of unknown events |
| `total_event_count` | Total canonical Silver event count |

### 8. Acceptance criteria are end-to-end

- AC1: Given daotao.ai fixture events, when Bronze ingestion runs, then `bronze_events` contains `raw_payload`, source metadata, event-time metadata, and processing partition fields.
- AC2: Given daotao.ai Bronze records, when Silver normalization runs, then `silver_learning_events` contains required `LearningEvent` fields.
- AC3: Given `silver_learning_events`, when the Gold metric runs, then `gold_course_activity_summary` is produced without reading raw payloads directly.
- AC4: Changing daotao.ai event type mappings does not require modifying `src/learnlake/`.
- AC5: A static check fails if `src/learnlake/` contains daotao.ai or edX-specific patterns such as `daotao`, `edx.`, `/courses/`, `seq_next`, `textbook.pdf`, `special_exam`, `problem.submitted`, `hocbk`, `soict`, or `course-v1:`.

## Risks / Trade-offs

- [Mapping engine becomes too generic] -> Limit v1 to path, const, coalesce, cast, resolver, and registry plugin operations.
- [Framework overclaims runtime independence] -> Document the framework as source-extensible but Spark/Delta-bound in this prototype.
- [daotao.ai semantics leak into core] -> Add static checks and keep daotao fixtures outside `src/learnlake/`.
- [Gold metric reads raw payload for convenience] -> Require Gold metric builders to consume `silver_learning_events`.
- [Scope expands back into layout migration] -> Defer Airflow/Docker/platform/serving migration to later changes.

## Migration Plan

1. Define contracts first: `BronzeEnvelope`, `LearningEvent`, `SourceProfile`, `MappingSpec`, quality rule spec, and metric execution profile.
2. Create minimal `src/learnlake/` skeleton and imports.
3. Implement only the runtime/config/storage pieces required for the daotao.ai vertical slice.
4. Create daotao.ai catalog profile, event type map, mapping, quality rules, metric definition, and fixtures.
5. Implement Bronze ingestion into `bronze_events`.
6. Implement Silver normalization into `silver_learning_events`.
7. Implement `gold_course_activity_summary`.
8. Add contract, unit, static-boundary, and integration tests.
9. Add thin app entrypoints for replay, Bronze, Silver, and Gold.
10. Leave broad runtime layout migration and cleanup for follow-up changes.

Rollback strategy:

- Revert this change if the vertical slice cannot pass tests.
- Do not remove old Spark/Kafka folders in this change; old runtime remains available while the new boundary is proven.

## Open Questions

- Which exact daotao.ai fixture subset should be used for the first contract test?
- Should `raw_payload` be stored as a Spark `string` or parsed `struct/map` in the first Bronze table implementation?
- Should `gold_course_activity_summary` use fixed windows only, or also support batch aggregation over fixture data for tests?
