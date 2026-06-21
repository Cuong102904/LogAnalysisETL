## Why

The repository needs a real framework boundary before any large layout migration. The first implementation should prove that daotao.ai can run through reusable contracts, catalog-driven mapping, canonical Silver output, quality validation, and one Gold metric without embedding daotao.ai or edX-specific rules in framework core.

## What Changes

- Introduce a minimal `src/learnlake/` package for a source-extensible learning-log analytics framework implemented on the existing Spark/Delta lakehouse runtime.
- Define explicit contracts for `BronzeEnvelope`, `LearningEvent`, `SourceProfile`, `MappingSpec`, quality rules, and metric execution profiles.
- Use one `bronze_events` table for the first implementation, partitioned by `source_id` and `processing_date`; per-source Bronze tables are deferred.
- Create daotao.ai catalog files for source profile, field mapping, edX tracking-log event type mapping, quality rules, and one Gold metric.
- Implement only the mapping operations needed for the vertical slice: `path`, `const`, `coalesce`, `cast`, `resolver`, and approved `plugin` calls from an internal registry.
- Do not support arbitrary Python imports, arbitrary SQL/Python expressions in YAML, a full CLI, full EdNet pipeline, or full runtime/platform folder migration in this change.
- Add thin Spark app entrypoints for Bronze, Silver, and Gold vertical-slice execution while leaving broad Airflow, Docker, Trino, Superset, and platform migration to a later change.
- Add contract, unit, and integration tests that prove daotao.ai fixture events produce valid `LearningEvent` rows and one Gold metric without modifying `src/learnlake/`.

## Capabilities

### New Capabilities

- `bronze-event-envelope`: Defines the common Bronze envelope fields and table behavior for raw source preservation.
- `canonical-learning-event`: Defines the canonical Silver `LearningEvent` fields, semantics, quality status, and relationship to optional projection tables.
- `source-profile-catalog`: Defines source profiles, source identity versus source type, catalog file references, and the rule that source-specific declarations live outside core.
- `mapping-normalization-engine`: Defines the supported mapping operations, resolver behavior, plugin registry boundary, timestamp handling, and daotao.ai normalization flow.
- `quality-validation-engine`: Defines common and source-specific quality validation behavior for canonical Silver outputs.
- `gold-metric-builder`: Defines the first Gold metric vertical slice and the rule that Gold reads canonical Silver instead of raw payloads.
- `analysis-execution-profile`: Defines lightweight metric execution metadata such as mode, priority, trigger interval, enabled state, and resource hints without implementing a scheduler.
- `thin-runtime-entrypoints`: Defines the required thin Spark and replay entrypoints for the vertical slice without migrating the full local platform layout.

### Modified Capabilities

- None. No archived OpenSpec specs exist yet. This change intentionally narrows and supersedes the broader draft change `restructure-repo-as-learnlake-framework`; it does not archive or modify an existing spec capability.

## Impact

- Affected code: `pyproject.toml`, new `src/learnlake/` package, selected reusable logic from `spark/`, selected replay/runtime entrypoints, top-level test organization, and daotao.ai fixtures.
- Affected data contracts: new `BronzeEnvelope`, canonical `LearningEvent`, `SourceProfile`, `MappingSpec`, quality rule, and metric execution profile contracts.
- Affected runtime: minimal Spark app entrypoints for `run_bronze.py`, `run_silver.py`, and `run_gold.py`; broad Airflow/Docker/platform/serving path migration is deferred.
- Affected tests: new contract tests, mapper/resolver/quality unit tests, static core-boundary checks, and daotao.ai Bronze-to-Silver-to-Gold vertical-slice tests.
