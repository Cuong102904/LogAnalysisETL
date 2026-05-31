## Context

The Spark package currently uses a mostly clean architecture shape, but the practical boundaries are still blurry. Entry points, orchestration, domain logic, schemas, and shared helpers are spread across `apps/`, `domain/`, `infrastructure/`, `utils/`, and `domain/schemas/`, which makes the codebase harder to reason about and harder to evolve safely.

This change is primarily an organization refactor, not a runtime feature change. The key constraint is to improve code locality and ownership without forcing Airflow, docker compose, or operational scripts to change their submission targets.

## Goals / Non-Goals

**Goals:**

- Establish a clear folder model for Spark code by responsibility.
- Keep runnable entrypoints stable under `spark/apps/*/main.py`.
- Move orchestration into `pipelines/` so `apps/` remains thin.
- Make `schemas/` the canonical home for data shapes.
- Preserve technical adapter boundaries in `infrastructure/`.
- Reduce ambiguity around shared helpers versus domain-specific logic.

**Non-Goals:**

- Changing Spark runtime behavior or optimizing query performance.
- Renaming the Spark runtime submit targets used by Airflow or compose.
- Redesigning Kafka topics, Delta table names, or lakehouse storage contracts.
- Introducing new external dependencies.

## Decisions

### 1. Keep `spark/apps/*/main.py` as the runtime entrypoint surface

**Rationale:** This minimizes the blast radius to Airflow, docker compose, and local smoke scripts. The refactor should improve internal organization without forcing deployment-layer changes.

**Alternatives considered:**

- Renaming entrypoints to `jobs/` or another new root folder.
  - Rejected because it would require broader path updates outside Spark.
- Moving execution responsibility into Airflow.
  - Rejected because Airflow already acts as orchestration around Spark, not ownership of the long-running streams.

### 2. Introduce `pipelines/` for orchestration

**Rationale:** The current `job.py` files already coordinate reads, transforms, and writes. Making that role explicit clarifies that orchestration is distinct from business logic.

**Alternatives considered:**

- Keeping orchestration inside `apps/*/job.py`.
  - Rejected because it keeps the boundary implicit and makes the entrypoint layer too heavy.
- Folding orchestration into `domain/`.
  - Rejected because domain modules should remain focused on transformations, not job wiring.

### 3. Move schema definitions into a top-level `schemas/` tree

**Rationale:** Schema definitions are a cross-cutting contract and deserve a single canonical home. A top-level `schemas/` folder is easier to discover than `domain/schemas/`, especially when the folder is used by Bronze, Silver, and Gold alike.

**Alternatives considered:**

- Keeping schemas under `domain/schemas/`.
  - Rejected because it makes schemas look like an implementation detail of domain logic.
- Using `contracts/` instead of `schemas/`.
  - Rejected for this repo because `schemas` is more concrete and aligns with current language in the codebase.

### 4. Keep `domain/` feature-oriented and business-focused

**Rationale:** Feature families such as video anomaly, learning journey, quiz performance, and exam anomaly are easier to maintain when their logic remains grouped by domain concern. Shared gold helpers can stay in `domain/gold/common/` if they are gold-specific.

**Alternatives considered:**

- Flattening all gold code into one shared module.
  - Rejected because it would reintroduce a large mixed-responsibility file set.

### 5. Keep generic helpers out of `domain/`

**Rationale:** Utility code that is not domain-specific belongs in `shared/` or `infrastructure/`, depending on whether it is generic or adapter-oriented. This keeps domain modules focused on business meaning.

**Alternatives considered:**

- Keeping everything in `utils/`.
  - Rejected because `utils/` tends to become a catch-all and obscures ownership.

### 6. Keep `configs/` as the runtime configuration and rules layer

**Rationale:** YAML app configs, rule sets, and storage paths are operational concerns, not schema or business logic. Keeping them separate avoids mixing data contracts with runtime parameters.

**Alternatives considered:**

- Embedding more configuration inside Python modules.
  - Rejected because it makes environment-specific behavior harder to inspect and override.

### 7. Update the Spark Docker image to include the new package roots

**Rationale:** The refactor only works at runtime if the Spark image contains the same package roots that the source tree exposes. Compose and standalone Spark jobs import from the container filesystem, so `pipelines/`, `schemas/`, and `shared/` must be copied into `/opt/project/spark` alongside the existing runtime packages.

**Alternatives considered:**

- Relying on host bind mounts or Python path hacks at runtime.
  - Rejected because the repo already treats the Spark image as the runtime artifact for compose and submit targets.
- Leaving the Docker image unchanged.
  - Rejected because the new package layout would fail at import time inside the container.

## Risks / Trade-offs

- [Import churn] → Mitigate by moving modules in a staged way and updating tests/docs alongside the code.
- [Boundary confusion between `shared/` and `domain/`] → Mitigate by keeping `shared/` strictly generic and leaving business-aware helpers in `domain/`.
- [Inconsistent migration state] → Mitigate by keeping `main.py` stable and updating references only after the new structure is in place.
- [Over-fragmentation] → Mitigate by limiting folder depth to responsibility-based grouping only, not naming-driven nesting.

## Migration Plan

1. Create the new top-level folder structure conceptually: `pipelines/`, `schemas/`, `shared/`.
2. Move orchestration out of `apps/*/job.py` into `pipelines/` while keeping `main.py` entrypoints unchanged.
3. Move schema files from `domain/schemas/` into `schemas/` and update imports.
4. Reclassify helper modules so generic helpers move to `shared/` and adapter code stays in `infrastructure/`.
5. Update the Spark Docker image to copy the new package roots into the runtime image and rebuild compose services.
6. Update docs, runbooks, and architecture references to reflect the new layout.
7. Run tests and smoke checks to confirm submission paths and import graphs still work.

Rollback strategy:

- Keep entrypoints stable throughout the migration.
- If import issues appear, temporarily preserve compatibility wrappers or re-export modules until all references are updated.

## Open Questions

- Should `domain/gold/common/` remain as a gold-specific helper area, or should some of its content move to `shared/`?
- Should compatibility re-export modules be kept temporarily after the move, or should imports be updated all at once?
- Should `apps/maintenance/` remain as an app-level entrypoint plus pipeline, or be handled as a special case alongside the other Spark jobs?
