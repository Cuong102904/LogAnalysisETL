## Context

The current repository is a working data platform prototype, but its top-level organization is still technology-first. Spark owns most transformation logic, Kafka owns replay/simulation code, Airflow owns scheduling wrappers, and serving/runtime folders live as independent top-level areas. The previous Spark refactor established cleaner internal Spark boundaries (`apps`, `pipelines`, `domain`, `schemas`, `infrastructure`, `shared`), but it did not establish a repository-wide framework boundary.

The target architecture is a reusable learning-log analytics framework with daotao.ai as the first case study. daotao.ai is an edX-based MOOC platform, so the current event names and rules such as `edx.grades.problem.submitted`, `edx.special_exam.*`, `/courses/`, `seq_next`, and `textbook.pdf.*` are source-format logic. They must not become part of framework core.

## Goals / Non-Goals

**Goals:**

- Introduce `src/learnlake/` as the source-agnostic Python framework package.
- Define stable public APIs for Bronze ingestion, Silver normalization, quality validation, metric building, and runtime configuration.
- Define common contracts for Bronze event envelopes and canonical Silver `LearningEvent` records.
- Move source-specific declarations into `catalog/` as source profiles, mappings, event type maps, quality rules, and metric definitions.
- Treat `projects/daotao_ai/` as the first concrete case study with documentation, data dictionary, samples, and optional source-specific transform plugins.
- Move replay code into `apps/replay/` as an application for simulating event-time streams from static datasets.
- Keep Spark, Kafka, Airflow, MinIO, Hive metastore, Trino, and Superset as runtime/platform concerns.
- Add contract tests that prove daotao.ai normalizes into the canonical `LearningEvent` model without hard-coding daotao.ai logic in `src/learnlake/`.

**Non-Goals:**

- Implementing full EdNet support in this change.
- Supporting storage formats beyond Delta Lake on MinIO.
- Replacing Spark Structured Streaming with another processing engine.
- Replacing Kafka as the local streaming runtime.
- Preserving old import paths or old runtime entrypoint paths indefinitely.
- Building a fully generic expression language that can express every future source-specific transformation without plugins.

## Decisions

### 1. Use `src/learnlake/` as the framework package

`learnlake` will use a standard Python `src` layout so imports resolve through package installation or configured project paths rather than accidentally through the repository working directory. This makes the package boundary explicit and prepares the codebase for real packaging.

Alternatives considered:

- Top-level `learnlake/`: simpler to create, but less strict about import hygiene.
- Keeping framework code under `spark/`: preserves current runtime paths, but keeps Spark as the mental owner of business logic.

### 2. Define SourceProfile as the framework boundary for each source

Each source will be described by a source profile in `catalog/sources/<source_id>.yaml`. A profile links input connector configuration, event-time field, Bronze settings, Silver mapping, event type maps, quality rules, metric selections, and checkpoints.

The framework runs a source profile; it does not import source-specific logic by name. Optional custom transforms may be referenced explicitly by profile or mapping configuration as plugin hooks.

Alternatives considered:

- Hard-code source branches in Python entrypoints: easier initially, but prevents framework reuse.
- Keep each source as a separate pipeline implementation: clear per-source control, but duplicates ingestion, normalization, quality, and metric logic.

### 3. Keep daotao.ai and edX rules outside core

edX/MOOC event names, URL patterns, and classification rules will move to catalog files or daotao.ai project plugins. `src/learnlake/normalization` will provide a mapper, expression evaluator, event type resolver, deduplication support, and normalizer runner. It will not contain `edx.*`, daotao.ai topic names, dataset file names, or hard-coded Delta paths.

Alternatives considered:

- Move `spark/domain/silver` directly into `learnlake/normalization`: rejected because much of that code is source-specific and would make the package a renamed daotao.ai pipeline.
- Delete all existing Silver logic and start from scratch: rejected because useful rules can be migrated into declarative mappings and tests.

### 4. Use a common Bronze table envelope and canonical Silver table

Bronze will use a common event envelope that preserves raw payload, source identity, event time, ingestion metadata, Kafka metadata where available, and schema version. The preferred table model is one `bronze_events` table partitioned by `source_id` and processing date.

Silver will center on a required `silver_learning_events` table. Optional Silver projection tables such as video interactions, assessment events, navigation events, and system events can be built from `silver_learning_events` or from standardized extension contracts. Gold metrics should not read directly from raw source payloads.

Alternatives considered:

- Per-source Bronze tables only: simpler isolation, but weaker framework story.
- Multiple Silver tables without a canonical `LearningEvent`: preserves current shape, but makes metric builders source-specific.

### 5. Keep executable apps thin

`apps/spark/run_bronze.py`, `apps/spark/run_silver.py`, and `apps/spark/run_gold.py` will parse arguments, load a source profile or metric profile, create runtime objects, and call `learnlake` APIs. `apps/replay/` will simulate source streams from static files and publish to Kafka. `apps/bootstrap/` will create runtime resources such as topics, buckets, and tables.

Alternatives considered:

- Keep old `spark/apps/*/main.py` as canonical: preserves compatibility, but conflicts with the framework-first structure.
- Move runtime orchestration into Airflow code: rejected because Airflow should schedule jobs, not own transformation behavior.

### 6. Separate runtime platform from serving and orchestration

The local platform will live under `platform/local/` and contain Docker/runtime configuration for Kafka, Spark, MinIO, Hive metastore, Trino, and Superset where appropriate. Airflow DAGs and tasks will live under `orchestration/airflow/`. Trino/Superset views and bootstrap logic will live under `serving/`.

This separation keeps platform composition, orchestration, and BI serving from appearing as framework logic.

### 7. Make contract tests part of the framework proof

The change will add tests that validate source profile schema, mapping schema, event type resolution, required `LearningEvent` fields, quality rule behavior, and daotao.ai fixture normalization. The key invariant is: adding a new source profile and fixtures must not require code changes under `src/learnlake/`.

## Risks / Trade-offs

- [Large breaking refactor] -> Mitigate by implementing a vertical slice first: daotao.ai replay to Kafka, Bronze ingestion, Silver normalization, quality validation, and one Gold metric.
- [Over-generic mapping engine] -> Mitigate by supporting a small expression/path/constant/resolver model first, with explicit plugin hooks for source-specific transforms.
- [Hidden daotao.ai logic leaking into core] -> Mitigate with tests and code review checks that reject daotao.ai, edX event names, and hard-coded source paths in `src/learnlake/`.
- [Runtime path churn] -> Mitigate by updating Docker Compose, Airflow task commands, Spark submit commands, docs, and smoke tests in the same change.
- [Canonical Silver model loses useful current detail] -> Mitigate by keeping optional semantic projection tables derived from canonical Silver instead of deleting all specialized concepts.
- [EdNet not implemented yet] -> Mitigate by designing SourceProfile and mapping contracts with EdNet-compatible fields, and optionally adding minimal fixture-only contract coverage later.

## Migration Plan

1. Create the target repository skeleton: `src/learnlake/`, `catalog/`, `apps/`, `projects/`, `platform/local/`, `orchestration/`, `serving/`, and top-level `tests/`.
2. Move generic runtime helpers, Spark session creation, config loading, storage paths, Delta I/O, Kafka connectors, and file connectors into `src/learnlake/`.
3. Define Bronze envelope and canonical `LearningEvent` contracts under `src/learnlake/contracts/`.
4. Create daotao.ai source profile, mapping, event type map, quality rules, and metric definitions under `catalog/`.
5. Convert existing edX/MOOC classifier and normalizer rules into declarative daotao.ai catalog files where possible.
6. Add plugin hooks only for source-specific transforms that cannot be expressed declaratively.
7. Create thin Spark apps that run Bronze, Silver, and Gold through `learnlake` APIs.
8. Move Kafka replay code into `apps/replay/` and keep daotao.ai dataset parsing under replay source adapters or project plugins.
9. Move Airflow, serving, and local platform files to their target folders and update runtime references.
10. Add contract, unit, and integration tests for the daotao.ai vertical slice.
11. Remove old technology-first folders and stale compatibility paths once the new vertical slice passes tests and smoke checks.

Rollback strategy:

- This is a breaking repo restructure, so rollback is a git revert of the change before archiving.
- During implementation, keep migration commits grouped by boundary so partial issues can be isolated to package, catalog, apps, platform, or tests.

## Open Questions

- Should `bronze_events` be the only Bronze table for the thesis prototype, or should the framework support optional per-source Bronze tables from the first implementation?
- Which Silver projection tables are required for the daotao.ai use case in the first vertical slice: video, assessment, navigation, system, or only canonical learning events plus one metric?
- Should CLI commands under `learnlake` be implemented immediately, or should the first implementation expose only `apps/spark/run_*.py` and add CLI later?
- How strict should plugin loading be for custom transforms: Python import path from YAML, registered entrypoints, or a small internal registry?
