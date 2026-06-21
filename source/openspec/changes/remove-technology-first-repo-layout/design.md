## Context

The codebase already contains the beginnings of a framework-first layout through `src/learnlake/`, `catalog/`, `apps/`, and `projects/daotao_ai/`, but it still keeps legacy top-level folders such as `spark/`, `kafka/`, `airflow/`, `trino/`, and `superset/`. Those folders are no longer just infrastructure assets: some still carry runtime entrypoints, use-case logic, or documentation that makes them look like the true owners of system behavior.

This creates a hybrid repository model. Contributors must infer whether logic belongs to framework core, use-case code, runtime adapter code, or platform assets, and the answer changes by folder. Gold logic is especially exposed: some logic is too use-case-specific to force into a generic framework builder, yet the current layout still suggests Spark app folders own it.

The target state is a responsibility-first repository where:

- `src/learnlake/` owns generic framework behavior
- `catalog/` owns declarative definitions
- `projects/<usecase>/` owns use-case semantics, custom transforms, and use-case-specific Gold code
- `apps/` owns thin executable entrypoints only
- `platform/local/` owns infrastructure/runtime assets
- `orchestration/` owns schedulers and workflow runners
- `serving/` owns query and BI assets

This change also introduces a declarative workflow-definition surface so execution order and parallelism can be described by configuration instead of hand-coded inside technology-specific folders.

## Goals / Non-Goals

**Goals:**

- Remove technology-first ownership from the repository root.
- Define a final target structure where top-level folders reflect responsibility rather than implementation technology.
- Move remaining reusable logic into `src/learnlake/` and move use-case-specific logic into `projects/<usecase>/`.
- Introduce workflow configuration for task dependency graphs, execution order, and parallel stages.
- Define where Gold business logic lives when it is too specific to be represented as a generic framework metric builder.
- Migrate platform, orchestration, and serving assets into explicit boundaries.
- Remove legacy top-level folders such as `spark/`, `kafka/`, `airflow/`, `trino/`, and `superset/` after migration is complete.

**Non-Goals:**

- Making every Gold metric generic or declarative.
- Replacing Spark, Kafka, Airflow, Trino, Superset, MinIO, or Hive Metastore as technologies.
- Designing a distributed workflow scheduler from scratch inside framework core.
- Supporting every possible workflow policy in the first iteration.
- Preserving old top-level folder paths indefinitely after migration.

## Decisions

### 1. Use responsibility-first top-level ownership

The repository root will be organized by responsibility boundaries, not by technology names. This is the only way to make folder names communicate architectural ownership correctly.

Target ownership:

- `src/learnlake/`: framework core
- `catalog/`: source, mapping, quality, metric, environment, and workflow declarations
- `apps/`: thin entrypoints and bootstrap tools
- `projects/`: use-case packs
- `platform/local/`: local deployment assets and service configs
- `orchestration/`: Airflow and other workflow runners
- `serving/`: Trino, Superset, and semantic serving assets
- `tests/`: verification

Alternatives considered:

- Keep current technology folders but document them as “platform only”: rejected because the folder names still advertise the wrong ownership model.
- Move only part of the tree and keep `spark/` as a stable umbrella: rejected because it preserves a misleading runtime-first mental model.

### 2. Delete legacy technology folders after migration, not before

Legacy top-level folders will be removed only after all remaining responsibilities inside them have a clear destination and all runtime paths, docs, and tests have been updated.

This keeps deletion as a verifiable completion criterion instead of an optimistic cleanup step. The end state is not “mostly migrated”; the end state is “no supported path depends on legacy top-level technology ownership.”

Alternatives considered:

- Delete folders immediately and rebuild structure opportunistically: rejected because it creates path churn without preserving traceable ownership migration.
- Keep folders empty as placeholders: rejected because they continue to pollute the top-level model and invite regression.

### 3. Keep `apps/` thin and move use-case-specific Gold logic into `projects/<usecase>/`

Gold logic is not uniformly generic. Some metrics and projections can be handled by shared framework helpers, but many Gold behaviors are use-case-specific and should remain explicit code owned by the use case.

Therefore:

- `apps/spark/` remains a runtime adapter layer only
- generic framework metric helpers stay under `src/learnlake/metrics/`
- use-case-specific Gold builders live under `projects/<usecase>/gold/`
- use-case-specific transforms and extractors live under `projects/<usecase>/`

Alternatives considered:

- Keep `spark/apps/gold_*` as the home for Gold logic: rejected because it makes Spark look like the business owner.
- Force all Gold logic into generic framework declarations: rejected because it over-generalizes code that is inherently use-case-specific.

### 4. Add declarative workflow definitions for dependency ordering and parallel stages

The framework will support a configuration surface, likely under `catalog/workflows/` and/or `projects/<usecase>/workflows/`, that defines task IDs, dependency edges, execution stage order, and parallelizable branches.

The framework responsibility is to understand and validate the graph, expose ordered execution plans, and allow runtime adapters or orchestrators to execute those plans. It is not required to become a full scheduler in the first step.

Examples of supported concepts:

- task identity
- `after` dependencies
- stage grouping
- priority or ordering hints
- optional materialization targets
- retry/checkpoint policy references

Alternatives considered:

- Leave all ordering logic in Airflow DAG Python code: rejected because the workflow semantics become runtime-specific.
- Put workflow ordering directly into app entrypoints: rejected because it duplicates orchestration semantics across runtimes.

### 5. Treat infrastructure technologies as platform, orchestration, or serving assets only

Spark, Kafka, MinIO, Hive Metastore, Trino, Superset, and Airflow remain important, but only as runtime or operational layers.

Responsibility mapping:

- Spark/Kafka/MinIO/Hive service configs and images -> `platform/local/`
- Airflow DAGs/operators/config -> `orchestration/airflow/`
- Trino views/bootstrap and Superset bootstrap/datasets -> `serving/`

This keeps technology-specific setup visible without allowing it to own business semantics.

Alternatives considered:

- Flatten all infrastructure files directly under `platform/local/` without service grouping: rejected because service-specific assets become hard to navigate.
- Keep `trino/` and `superset/` separate because they are “serving-ish”: rejected because serving is the real responsibility boundary.

## Risks / Trade-offs

- [Migration touches many paths at once] -> Mitigation: migrate by responsibility slice, then delete folders only after runtime/docs/tests point exclusively to the new layout.
- [Gold logic boundary becomes inconsistent across use cases] -> Mitigation: define explicit rules for what belongs in `src/learnlake/metrics/` versus `projects/<usecase>/gold/`.
- [Workflow config grows into a second orchestration system] -> Mitigation: keep the first version limited to dependency graph semantics and execution planning, not full distributed scheduling.
- [Legacy references remain in docs or Docker mounts] -> Mitigation: treat documentation and runtime path rewiring as first-class tasks before folder deletion.
- [Developers reintroduce technology-owned logic later] -> Mitigation: add tests or static checks that reject business logic in legacy or forbidden locations after migration.

## Migration Plan

1. Define the final responsibility-first structure and map every legacy folder responsibility to a new owner.
2. Move remaining framework-reusable logic into `src/learnlake/` and move use-case-specific Gold and transform logic into `projects/<usecase>/`.
3. Introduce workflow-definition specs and thin runners that consume them.
4. Relocate infrastructure assets to `platform/local/`, Airflow assets to `orchestration/airflow/`, and BI/query assets to `serving/`.
5. Update Docker Compose, runtime scripts, Airflow tasks, docs, and tests to use only new paths.
6. Verify no supported runtime path depends on top-level `spark/`, `kafka/`, `airflow/`, `trino/`, or `superset/`.
7. Delete the legacy top-level folders and any compatibility wrappers that still encode technology-first ownership.

Rollback strategy:

- Because this is a breaking repository restructure, rollback is a git revert of the change set before legacy folder deletion is finalized.
- Migration commits should remain grouped by boundary so regressions can be isolated to framework, project, platform, orchestration, or serving moves.

## Open Questions

- Should workflow definitions live entirely in `catalog/workflows/`, entirely in `projects/<usecase>/workflows/`, or use a split model where catalog owns generic workflow shapes and projects own use-case overrides?
- Should `apps/run_task.py` become the single generic runner for workflow tasks, or should Bronze/Silver/Gold keep separate thin entrypoints plus a generic task runner?
- Which current Gold jobs are generic enough to remain framework builders, and which should be reclassified immediately as `projects/daotao_ai/gold/*`?
- Do we want a static enforcement rule that outright forbids new Python business logic under `platform/`, `orchestration/`, and `serving/`, or only under removed legacy folders?
