## Why

The repository is still split across technology-owned top-level folders such as `spark`, `kafka`, `airflow`, `trino`, and `superset`, even though the framework-first boundary has already started to emerge under `src/learnlake`, `catalog`, `apps`, and `projects`. This hybrid state keeps ownership unclear, spreads business logic across runtime-specific folders, and makes every new use case look like a patchwork of platform implementations instead of a framework with explicit extension points.

## What Changes

- **BREAKING**: Restructure the repository into a responsibility-first layout centered on `src/learnlake/`, `catalog/`, `apps/`, `projects/`, `platform/local/`, `orchestration/`, `serving/`, and `tests/`.
- **BREAKING**: Remove top-level technology-owned folders such as `spark/`, `kafka/`, `airflow/`, `trino/`, and `superset/` after their remaining responsibilities have been migrated to framework, project, platform, orchestration, or serving boundaries.
- **BREAKING**: Move any remaining reusable runtime or semantic logic out of technology folders and make `src/learnlake/` the only owner of generic framework behavior.
- Introduce a workflow-definition surface that lets a use case declare task dependencies, execution order, and parallelizable stages through configuration instead of embedding orchestration semantics in runtime-specific code.
- Define `projects/<usecase>/` as the owner of source-specific semantics, custom transforms, and Gold business logic that cannot or should not be genericized into the framework.
- Treat Spark, Kafka, Airflow, Trino, Superset, MinIO, and Hive Metastore strictly as execution infrastructure, scheduler runtime, or serving assets, not as owners of business rules.
- Update runtime entrypoints, Docker/runtime paths, Airflow scheduling, serving assets, and documentation so the new boundaries are the only documented and supported structure.

## Capabilities

### New Capabilities

- `responsibility-first-repository-layout`: Defines the target repository structure, ownership boundaries, migration rules, and final removal of technology-first top-level folders.
- `declarative-workflow-definition`: Defines configuration-driven task graphs for per-use-case execution ordering, dependencies, and parallel stages.
- `usecase-owned-gold-and-transforms`: Defines how source-specific transforms and Gold business logic live under `projects/<usecase>/` while framework code remains generic.

### Modified Capabilities

- None.

## Impact

- Affected code: top-level repository structure, `src/learnlake/`, `apps/`, `projects/`, `spark/`, `kafka/`, `airflow/`, `trino/`, `superset/`, `minio/`, `hive-metastore/`, `deploy/`, runtime scripts, and test layout.
- Affected APIs: runtime entrypoint paths, workflow configuration paths, project plugin locations, Gold builder loading boundaries, Docker mount paths, and Airflow task invocation paths.
- Affected systems: local Docker Compose topology, Spark submit commands, Kafka replay/bootstrap flows, Airflow DAG/task wiring, Trino bootstrap/view loading, Superset bootstrap assets, and repo documentation.
- Affected developer workflow: new use cases must be added through `catalog/`, `projects/`, workflow definitions, and optional project code without creating or extending top-level technology-owned logic folders.
