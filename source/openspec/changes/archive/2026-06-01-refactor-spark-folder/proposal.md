## Why

The current Spark package mixes entrypoints, orchestration, domain logic, schema definitions, and helper utilities in a way that makes ownership and dependency flow harder to follow. The codebase needs a clearer organization boundary before further Spark optimization work, so future changes stay localized and easier to test.

## What Changes

- Introduce a clearer Spark package layout with separate top-level areas for `apps/`, `pipelines/`, `domain/`, `schemas/`, `infrastructure/`, `shared/`, and `configs/`.
- Keep `apps/*/main.py` as the runtime entrypoint surface so Airflow, docker compose, and smoke scripts do not need to change their submit targets.
- Move orchestration logic out of `apps/*/job.py` into `pipelines/`.
- Move schema definitions out of `domain/schemas/` into a canonical top-level `schemas/` tree.
- Keep business transformations in `domain/` and generic technical helpers in `shared/` or `infrastructure/` based on responsibility.
- Update the Spark container image so it packages the refactored top-level Spark package roots used at runtime.
- Update documentation and references so the repo narrative matches the new organization.

## Capabilities

### New Capabilities
- `spark-code-organization`: The Spark repository SHALL expose a clearer folder structure and module boundary model for entrypoints, orchestration, domain logic, schemas, infrastructure, and shared helpers.

### Modified Capabilities
- None

## Impact

- Affected code: `spark/` package structure, internal imports, and test import paths.
- Affected documentation: repository README, Spark architecture/runbook docs, and any notes that reference old module paths.
- Affected runtime integrations: minimal, because Spark entrypoint paths remain under `spark/apps/*/main.py`, but the Spark image must include the refactored package roots.
- Affected developer workflow: contributors will place orchestration, schema, and helper code in more clearly defined folders.
