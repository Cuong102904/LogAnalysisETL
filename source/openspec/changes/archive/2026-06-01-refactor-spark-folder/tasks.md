## 1. Package structure

- [x] 1.1 Create the new top-level Spark package folders for `pipelines/`, `schemas/`, and `shared/`.
- [x] 1.2 Add or update `__init__.py` files so the new packages are importable without path hacks.
- [x] 1.3 Keep `spark/apps/*/main.py` as the stable entrypoint surface for all runnable Spark jobs.

## 2. Move orchestration out of apps

- [x] 2.1 Move Bronze read-transform-write coordination from `apps/bronze_ingestor/job.py` into a Bronze pipeline module.
- [x] 2.2 Move Silver stream coordination from `apps/silver_transformer/job.py` into a Silver pipeline module.
- [x] 2.3 Move Gold aggregation and alert coordination from `apps/gold_aggregator/job.py` and `apps/gold_alerting/job.py` into Gold pipeline modules.
- [x] 2.4 Update `apps/*/main.py` files to call the new pipeline modules without changing their runtime entrypoint names.

## 3. Rehome schemas and shared helpers

- [x] 3.1 Move schema definitions from `domain/schemas/` into the top-level `schemas/` tree.
- [x] 3.2 Update Spark domain modules to import schemas from the new canonical location.
- [x] 3.3 Move generic helper modules from `utils/` into `shared/` where they are not domain-specific.
- [x] 3.4 Keep domain-specific helpers in `domain/` and `domain/gold/common/` where they encode business meaning.

## 4. Update references and verify

- [x] 4.1 Update Spark tests and import paths so they resolve against the new package layout.
- [x] 4.2 Refresh repository docs and runbooks that reference old Spark file paths or folder names.
- [x] 4.3 Verify Airflow, docker compose, and smoke scripts still point at the unchanged `spark/apps/*/main.py` entrypoints.
- [x] 4.4 Run the Spark test suite and the documented smoke checks to confirm the refactor is behavior-preserving.
- [x] 4.5 Update `spark/Dockerfile` so the runtime image includes the refactored package roots (`pipelines/`, `schemas/`, `shared/`) used by the new layout.
