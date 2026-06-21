## 1. Canonical package structure

- [x] 1.1 Keep the top-level Spark package folders for `pipelines/`, `schemas/`, and `shared/` as the canonical layout.
- [x] 1.2 Keep `spark/apps/*/main.py` as the stable entrypoint surface for all runnable Spark jobs.
- [x] 1.3 Ensure the canonical package roots are importable without path hacks.

## 2. Orchestration and entrypoints

- [x] 2.1 Keep Bronze read-transform-write coordination in `pipelines/bronze/ingest_pipeline.py`.
- [x] 2.2 Keep Silver stream coordination in `pipelines/silver/transform_pipeline.py`.
- [x] 2.3 Keep Gold aggregation and alert coordination in `pipelines/gold/aggregation_pipeline.py` and `pipelines/gold/alert_pipeline.py`.
- [x] 2.4 Keep `apps/*/main.py` files as thin launchers that call the pipeline modules.

## 3. Canonical schemas and helpers

- [x] 3.1 Keep schema definitions in the top-level `schemas/` tree only.
- [x] 3.2 Update Spark domain modules to import schemas from the canonical location.
- [x] 3.3 Keep generic helper modules in `shared/` where they are not domain-specific.
- [x] 3.4 Keep domain-specific helpers in `domain/` and `domain/gold/common/` where they encode business meaning.

## 4. Remove legacy wrappers and update references

- [x] 4.1 Remove `domain/schemas/` after all imports point to `schemas/`.
- [x] 4.2 Remove `utils/` and update all imports to `shared/`.
- [x] 4.3 Remove `apps/*/job.py` wrappers and keep `main.py` as the only runtime entrypoint surface.
- [x] 4.4 Update Spark tests and import paths so they resolve against the canonical package layout.
- [x] 4.5 Refresh repository docs and runbooks that reference old Spark file paths or folder names.
- [x] 4.6 Update `spark/Dockerfile` so the runtime image includes only the canonical package roots used by the new layout.
- [x] 4.7 Run the Spark test suite and the documented smoke checks to confirm the refactor is behavior-preserving.
