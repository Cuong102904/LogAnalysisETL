## Context

The repository currently contains two Silver normalization directions:

- The framework path under `src/learnlake/` uses catalog-driven mapping to produce a single canonical `LearningEvent` table (`silver_learning_events`). This is source-agnostic but too generic for analytics because domain details remain in raw payloads or generic JSON.
- The legacy Spark path under `spark/domain/silver/normalizers/` already fans out into domain tables such as performance, video, PDF, navigation, exam, system, and unknown events. This is better for analytics but hard-codes Open edX/daotao.ai event semantics in Spark modules.

The target architecture should combine the strengths of both: a source-agnostic framework core with a multi-target Silver model and source packs that own source-specific routing and extraction.

Important constraints:

- `src/learnlake/` must not contain daotao.ai, Open edX, or dataset-specific identifiers.
- The source `time` field remains the source of truth for event time; line order and ingest order must not be used to infer event sequence.
- The daotao.ai source pack is the first implementation, but the design must leave room for EdNet, Moodle, xAPI, or other learning-log sources.
- The POC has not been run end-to-end yet, so breaking the current single-table Silver contract is acceptable if the replacement is coherent and testable.

## Goals / Non-Goals

**Goals:**

- Replace the single-output Silver normalizer with a multi-target normalization model.
- Keep one source-agnostic `silver_event_index` row per valid normalized source event.
- Emit typed domain fact rows for analytics-ready Silver tables when an event belongs to assessment, video, document/PDF, navigation, exam, course content, authoring, auth, system/noise, or unknown domains.
- Move Open edX/daotao.ai classification and extraction into `projects/daotao_ai` and catalog files.
- Support declarative routing rules that decide event group, normalized type, target tables, and extractor names.
- Keep plugin and extractor invocation controlled by an approved registry; YAML must not import arbitrary Python.
- Update Spark Silver runtime so a single Bronze input stream can write multiple Silver Delta targets with per-target schemas and checkpoints.
- Preserve raw lineage through `event_id`, `raw_event_ref`, source metadata, and Bronze retention.
- Include a final local Docker Compose verification path for Kafka, MinIO, Spark, Bronze, Silver, and tracking-log replay.

**Non-Goals:**

- Do not implement Gold feature redesign beyond adjusting Gold readers where needed for compatibility.
- Do not move the whole repository into the future platform layout in this change.
- Do not introduce a new scheduler; Airflow and Spark remain responsible for execution.
- Do not infer user sessions, causality, or attempt grouping in Silver. Sessionization and business-level event grouping belong in Gold or later feature jobs.
- Do not require every source to populate every domain table.

## Decisions

### Decision 1: Silver has an event index plus domain fact tables

Use `silver_event_index` as the canonical common table and domain tables as typed projections.

```text
bronze_events
  -> silver_event_index
  -> silver_assessment_events
  -> silver_video_events
  -> silver_document_events
  -> silver_navigation_events
  -> silver_exam_events
  -> silver_course_content_events
  -> silver_authoring_events
  -> silver_auth_events
  -> silver_system_events
  -> silver_unknown_events
```

Rationale:

- `silver_event_index` provides lineage, common dimensions, normalized event semantics, quality status, and a stable join key.
- Domain tables keep analytics fields typed and queryable without reparsing raw JSON.
- Sparse domain-specific fields do not bloat a single wide table.
- Sources with limited event coverage can emit only relevant domain facts.

Alternatives considered:

- Keep only one generic `LearningEvent` table. Rejected because Gold still needs source-specific JSON parsing and domain validation is weak.
- Keep only domain tables. Rejected because lineage, auditing, cross-domain event counts, and unknown/noise handling need a common index.

### Decision 2: Rename or supersede `LearningEvent` with `EventIndex`

The current `LearningEvent` contract should be replaced or treated as a compatibility alias for `EventIndex`.

Rationale:

- The current name suggests the table contains all learning semantics, but it is actually a common event envelope.
- `EventIndex` better describes the role: one row per normalized event, shared join key, common fields, and routing metadata.

Migration choice:

- During POC, introduce `EventIndex` and update callers.
- If needed, keep a temporary compatibility export named `LearningEvent = EventIndex` only for tests or old code, but do not keep `silver_learning_events` as the long-term table name.

### Decision 3: Source packs own source-specific routing and extraction

`projects/daotao_ai` should provide:

- Routing rules for Open edX tracking-log event patterns.
- Extractor functions for complex daotao.ai/Open edX payloads.
- Optional source-specific quality rules and fixtures.
- Documentation of mapping assumptions.

`src/learnlake` should provide:

- Rule loading and validation.
- Route matching primitives.
- Mapping/extractor execution contracts.
- Approved plugin registry.
- Multi-target normalization lifecycle.

Rationale:

- Adding EdNet or another source should not modify framework core.
- Open edX event names such as `problem_check`, `problem_graded`, `play_video`, or `textbook.pdf.*` are source-pack concerns.

Alternatives considered:

- Put all routing in `catalog/event_types/edx_tracking_log.yaml`. Rejected as insufficient because routing needs target tables and extractors, not just action/object/category.
- Keep Spark `if/else` classifier. Rejected because it is not a framework boundary and is difficult to reuse outside Spark.

### Decision 4: Routing is declarative, extraction is controlled code

Routing should be YAML-driven with constrained operators:

- `equals`
- `in`
- `starts_with`
- `contains`
- `regex`
- `all`
- `any`
- `not`
- optional payload/context key checks

Extraction should use either declarative field mappings for simple projections or approved extractor functions for complex source-specific payloads.

Rationale:

- Matching event patterns is readable and reviewable as data.
- Complex parsing such as Open edX block usage keys, URL-encoded form answers, or nested `event` payloads is safer in tested Python functions.
- The plugin registry prevents arbitrary import execution from YAML.

### Decision 5: Normalization returns `NormalizationResult`

The core normalizer should produce:

```python
NormalizationResult(
    event_index=<dict | None>,
    facts=[
        FactRecord(target_table="silver_video_events", record={...}),
        ...
    ],
    invalid_records=[...],
)
```

Rationale:

- One source event can have exactly one event-index row and zero or more fact rows.
- Invalid records can be retained without writing partial facts.
- Runtime writers can fan out by `target_table`.

### Decision 6: Spark runtime writes configured targets, not hard-coded normalizers

`SourceProfile.silver` should support multiple configured targets:

```yaml
silver:
  event_index:
    table: silver_event_index
    path: ...
    checkpoint: ...
    mapping: ...
  targets:
    assessment:
      table: silver_assessment_events
      path: ...
      checkpoint: ...
      schema: ...
    video:
      table: silver_video_events
      path: ...
      checkpoint: ...
      schema: ...
  invalid:
    table: silver_invalid_events
    path: ...
```

Rationale:

- Runtime stays generic.
- Table paths/checkpoints are source-profile concerns.
- Spark can add or remove domain sinks without code branches in the entrypoint.

### Decision 7: Silver stores selected source details, Bronze stores full raw

Domain fact tables store typed fields needed for analysis. Full raw payload remains in Bronze and is referenced by `raw_event_ref`.

Rationale:

- Avoid duplicating huge HTML payloads such as Open edX `problem_graded` rendered HTML in Silver.
- Keep traceability without making every Silver table a raw archive.
- Allow reprocessing when mappings evolve.

## Risks / Trade-offs

- [Risk] Multi-target writes increase Silver runtime complexity and checkpoint management.
  → Mitigation: store target definitions in the source profile, isolate per-target checkpoints, and add integration tests for partial target writes.

- [Risk] Python row-level extraction can be slow for high-volume streaming.
  → Mitigation: start with correctness for POC, but keep mapping/routing declarative enough to compile common paths to Spark expressions later.

- [Risk] Domain schema design may overfit daotao.ai.
  → Mitigation: define generic domain contracts and keep daotao-specific fields in optional payload/detail columns or source-specific extensions.

- [Risk] Breaking current `silver_learning_events` consumers can disrupt existing notebooks/views.
  → Mitigation: provide a temporary compatibility view or update consumers to read `silver_event_index` and domain fact tables.

- [Risk] Rule priority errors can misroute events, especially Open edX events where URL routes overlap semantic domains.
  → Mitigation: require explicit route priority and tests for known ambiguous events such as `/xblock/.../problem_check`, `/pdfbook/`, and `/api/edx_proctoring/`.

- [Risk] Sensitive auth fields can leak into Silver if raw OAuth query params are copied.
  → Mitigation: auth extractors must redact OAuth codes/tokens and Silver schemas should store booleans or provider names rather than secret values.

## Migration Plan

1. Introduce new contracts for `EventIndex`, `FactRecord`, `NormalizationResult`, and domain fact records under `src/learnlake/contracts`.
2. Extend source profile contracts to support `silver.event_index`, multiple `silver.targets`, and `silver.invalid`.
3. Add generic routing rule models and a router engine under `src/learnlake/normalization`.
4. Add extractor registry support that allows approved source-pack extractor functions.
5. Move daotao.ai Open edX classification patterns into `projects/daotao_ai/routing.yaml` or catalog-equivalent files.
6. Implement daotao.ai extractors for assessment, video, document/PDF, navigation, exam, course content, authoring, auth, system/noise, and unknown events.
7. Update `apps/spark/run_silver.py` to write event-index and fact targets from `NormalizationResult`.
8. Update tests and docs; keep any compatibility alias only if needed by existing POC assets.
9. Run unit, contract, and fixture integration tests.
10. Run the final Docker Compose smoke verification using the requested service subset.

Rollback strategy:

- Because this is POC-stage and not yet production, rollback means restoring the previous single-target mapping and `run_silver.py` behavior.
- Keep the old mapping file until the new multi-target path passes fixture and Docker Compose verification.
