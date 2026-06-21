## 1. Responsibility Map And Target Layout

- [x] 1.1 Define the final responsibility-first top-level structure in repo docs and change artifacts.
- [x] 1.2 Create a migration map from each legacy top-level folder (`spark`, `kafka`, `airflow`, `trino`, `superset`, `minio`, `hive-metastore`, `deploy`) to its new owner under `src/learnlake`, `apps`, `projects`, `platform/local`, `orchestration`, or `serving`.
- [x] 1.3 Identify every remaining business-logic or runtime entrypoint file that still makes a legacy technology folder look like an ownership boundary.

## 2. Framework And Use-Case Boundary Cleanup

- [x] 2.1 Move any remaining reusable runtime or semantic logic out of legacy technology folders and into `src/learnlake/`.
- [x] 2.2 Move source-specific transforms, enrichments, and semantic code into `projects/<usecase>/`.
- [x] 2.3 Reclassify current Gold implementations into either generic framework metrics under `src/learnlake/metrics/` or use-case-owned builders under `projects/<usecase>/gold/`.
- [x] 2.4 Remove or replace any legacy `spark/apps/*` Gold entrypoints so `apps/` remains the only supported runtime entrypoint surface.

## 3. Declarative Workflow Definition

- [x] 3.1 Define the workflow-definition schema for task IDs, dependency edges, execution grouping, and parallelizable branches.
- [x] 3.2 Implement workflow-definition loading and validation, including missing dependency and cycle detection.
- [x] 3.3 Expose runtime-agnostic execution planning that thin app runners or Airflow can consume.
- [x] 3.4 Add at least one use-case workflow definition file for the current daotao.ai pipeline.

## 4. Platform, Orchestration, And Serving Migration

- [x] 4.1 Move local runtime service assets for Spark, Kafka, MinIO, Hive Metastore, Trino, and Superset into `platform/local/`.
- [x] 4.2 Move Airflow DAGs, operators/tasks, and related config into `orchestration/airflow/`.
- [x] 4.3 Move Trino views/bootstrap assets and Superset bootstrap assets into `serving/`.
- [x] 4.4 Update Docker Compose, Dockerfiles, bootstrap scripts, and runtime mounts to use only the new paths.

## 5. Runtime Entry Points, Docs, And Tests

- [x] 5.1 Update `apps/` entrypoints and runtime commands so documented execution paths no longer depend on legacy top-level folders.
- [x] 5.2 Update README, architecture docs, and runbooks to teach only the responsibility-first layout.
- [x] 5.3 Add tests or static checks that detect forbidden business logic or supported runtime ownership under removed legacy technology folders.
- [ ] 5.4 Run affected unit, contract, integration, and smoke checks against the migrated paths.

## 6. Legacy Folder Removal

- [x] 6.1 Verify there are no supported code paths, docs, Docker mounts, or orchestration references left to top-level `spark/`, `kafka/`, `airflow/`, `trino/`, or `superset/`.
- [x] 6.2 Delete the migrated legacy top-level technology folders and any compatibility wrappers that preserve the old ownership model.
- [x] 6.3 Confirm the repository root now reflects only responsibility-first boundaries and that Gold logic, workflow definitions, platform assets, and serving assets resolve from their new owners.
