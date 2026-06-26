## 1. Gold Processing Modes

- [x] 1.1 Add a gold cadence registry/config that marks each output as `streaming` or `batch` and records its trigger policy.
- [x] 1.2 Refactor `projects/daotao_ai/gold/pipelines/aggregation_pipeline.py` so streaming outputs are started from a streaming-only path with the light micro-batch trigger.
- [x] 1.3 Create a batch gold entrypoint for `pdf_engagement_features`, `quiz_attempt_metrics`, and `user_learning_profile_daily` that materializes Delta tables on a scheduled run.
- [x] 1.4 Keep `anomaly_alerts` as a streaming downstream of `behavior_anomaly_signals` with the existing watermark and dedup behavior.
- [x] 1.5 Add unit tests that assert gold jobs read only silver inputs and keep output schema and partition keys stable.

## 2. Trino Semantic Contract

- [x] 2.1 Update the Trino view SQL files so each BI surface reads the correct gold table or tables while keeping the existing semantic view names.
- [x] 2.2 Ensure `course_improvement_view` and `learner_health_view` continue to compose only from gold semantic tables, not from silver inputs.
- [x] 2.3 Add SQL/bootstrap validation so semantic view dependencies fail fast if a gold table name or column contract changes.

## 3. Superset Dashboard Registry

- [x] 3.1 Refactor `serving/superset/bootstrap/bootstrap_superset.py` into a registry of dashboard surfaces, datasets, and chart sections or tabs.
- [x] 3.2 Implement `Live Ops` as one dashboard with multiple sections or tabs, 30-second auto-refresh, and manual refresh on demand.
- [x] 3.3 Implement `Learning Analytics` as a manual-refresh dashboard with filters, a wider layout, and drill-down sections.
- [x] 3.4 Add bootstrap smoke tests that verify dataset sources, dashboard names, section layout, and refresh settings.
- [x] 3.5 Add a Playwright fallback flow that logs into Superset and creates the declared dashboards, datasets, and sections if bootstrap/API creation does not materialize them automatically.

## 4. Documentation And Validation

- [x] 4.1 Update `docs/medallion.md`, `docs/trino.md`, and `docs/runbook.md` to describe the silver -> gold -> Trino -> Superset contract.
- [x] 4.2 Document which gold outputs are streaming versus batch and which dashboard surface consumes each one.
- [x] 4.3 Run an end-to-end smoke check against MinIO, Trino, and Superset after the change lands.
