## Why

The Spark package still contains legacy compatibility folders and duplicate wrapper modules from the earlier refactor. Entry points, orchestration, domain logic, schema definitions, and helper utilities are split between canonical package roots and leftover aliases, which makes ownership and dependency flow harder to follow. The codebase needs a single canonical organization boundary and the old compatibility layer needs to be removed so future changes stay localized and easier to test.

## What Changes

- Keep the canonical Spark package layout under the top-level areas `apps/`, `pipelines/`, `domain/`, `schemas/`, `infrastructure/`, `shared/`, and `configs/`.
- Keep `apps/*/main.py` as the runtime entrypoint surface so Airflow, docker compose, and smoke scripts do not need to change their submit targets.
- Move orchestration logic out of `apps/*/job.py` into `pipelines/` and remove the legacy `job.py` wrappers after callers are updated.
- Keep schema definitions only in the canonical top-level `schemas/` tree and remove `domain/schemas/`.
- Keep business transformations in `domain/` and generic technical helpers in `shared/` or `infrastructure/` based on responsibility.
- Remove the legacy `utils/` wrapper layer and any other compatibility helper aliases after callers import from `shared/` directly.
- Update the Spark container image so it packages only the canonical top-level Spark package roots used at runtime.
- Update documentation and references so the repo narrative matches the new organization and no longer treats legacy compatibility folders as part of the design.

## Capabilities

### New Capabilities
- `spark-code-organization`: The Spark repository SHALL expose a clearer folder structure and module boundary model for entrypoints, orchestration, domain logic, schemas, infrastructure, and shared helpers, with legacy wrapper folders removed once the canonical layout is in place.

### Modified Capabilities
- None

## Impact

- Affected code: `spark/` package structure, internal imports, and test import paths.
- Affected documentation: repository README, Spark architecture/runbook docs, and any notes that reference old module paths.
- Affected runtime integrations: minimal, because Spark entrypoint paths remain under `spark/apps/*/main.py`, but the Spark image must include the canonical package roots.
- Affected developer workflow: contributors will place orchestration, schema, and helper code in the canonical folders only, without maintaining legacy alias modules.
