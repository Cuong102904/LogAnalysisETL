## Context

The Spark package is currently close to the target clean architecture shape, but the practical boundaries are blurred by legacy compatibility folders. Entry points, orchestration, domain logic, schemas, and shared helpers are split across canonical folders and leftover aliases such as `domain/schemas/`, `utils/`, and `apps/*/job.py`. This change is an organization refactor, not a runtime feature change. The key constraint is to improve code locality and ownership while removing the compatibility layer instead of preserving it.

## Goals / Non-Goals

**Goals:**

- Establish a clear folder model for Spark code by responsibility.
- Keep runnable entrypoints stable under `spark/apps/*/main.py`.
- Move orchestration into `pipelines/` so `apps/` remains thin.
- Make `schemas/` the only canonical home for data shapes.
- Preserve technical adapter boundaries in `infrastructure/`.
- Remove legacy wrapper folders and duplicate aliases once the canonical layout is wired in.
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

**Rationale:** The current job wiring belongs in a dedicated orchestration layer. Making that role explicit clarifies that orchestration is distinct from business logic.

**Alternatives considered:**

- Keeping orchestration inside `apps/*/job.py`.
  - Rejected because it keeps the boundary implicit and makes the entrypoint layer too heavy.
- Folding orchestration into `domain/`.
  - Rejected because domain modules should remain focused on transformations, not job wiring.

### 3. Keep schema definitions only in the top-level `schemas/` tree

**Rationale:** Schema definitions are a cross-cutting contract and deserve one canonical home. A top-level `schemas/` folder is easier to discover than `domain/schemas/`, especially when the folder is used by Bronze, Silver, and Gold alike.

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

### 6. Remove legacy wrapper folders after migration

**Rationale:** The new architecture only becomes clear when the old alias folders are removed. Keeping `domain/schemas/`, `utils/`, and `apps/*/job.py` around after the migration would keep the codebase ambiguous and encourage new imports to drift back to the old structure.

**Alternatives considered:**

- Keeping compatibility wrappers indefinitely.
  - Rejected because it preserves the old mental model and keeps duplicate paths alive.
- Leaving legacy imports and folders in place but marking them deprecated.
  - Rejected because deprecation alone does not reduce the file-system and import-graph confusion.

### 7. Keep `configs/` as the runtime configuration and rules layer

**Rationale:** YAML app configs, rule sets, and storage paths are operational concerns, not schema or business logic. Keeping them separate avoids mixing data contracts with runtime parameters.

**Alternatives considered:**

- Embedding more configuration inside Python modules.
  - Rejected because it makes environment-specific behavior harder to inspect and override.

### 8. Update the Spark Docker image to include only canonical package roots

**Rationale:** The refactor only works at runtime if the Spark image contains the same package roots that the source tree exposes. Compose and standalone Spark jobs import from the container filesystem, so the image must copy the canonical roots and not keep packaging removed compatibility folders.

**Alternatives considered:**

- Relying on host bind mounts or Python path hacks at runtime.
  - Rejected because the repo already treats the Spark image as the runtime artifact for compose and submit targets.
- Leaving the Docker image unchanged.
  - Rejected because the new package layout would fail at import time inside the container.

## Risks / Trade-offs

- [Import churn] -> Mitigate by updating tests/docs alongside the code and deleting legacy folders only after imports are updated.
- [Boundary confusion between `shared/` and `domain/`] -> Mitigate by keeping `shared/` strictly generic and leaving business-aware helpers in `domain/`.
- [Inconsistent migration state] -> Mitigate by keeping `main.py` stable and updating references only after the new structure is in place.
- [Over-fragmentation] -> Mitigate by limiting folder depth to responsibility-based grouping only, not naming-driven nesting.

## Migration Plan

1. Keep the canonical top-level folder structure: `pipelines/`, `schemas/`, and `shared/`.
2. Remove orchestration wrappers from `apps/*/job.py` and keep `main.py` as the only runtime entrypoint surface.
3. Keep schema files only in `schemas/` and delete `domain/schemas/` after imports are updated.
4. Keep generic helper code only in `shared/` and delete the `utils/` wrapper layer.
5. Update the Spark Docker image to copy only the canonical package roots into the runtime image and rebuild compose services.
6. Update docs, runbooks, and architecture references to reflect the new layout and the removal of legacy folders.
7. Run tests and smoke checks to confirm submission paths and import graphs still work.

Rollback strategy:

- Keep entrypoints stable throughout the migration.
- If import issues appear, fix the canonical imports rather than restoring the legacy folders.

## Open Questions

- Should `domain/gold/common/` remain as a gold-specific helper area, or should some of its content move to `shared/`?
- Should `apps/maintenance/` remain as an app-level entrypoint plus pipeline, or be handled as a special case alongside the other Spark jobs?
