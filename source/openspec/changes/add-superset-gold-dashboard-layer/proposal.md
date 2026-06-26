## Why

The silver layer already contains real normalized event data, and the repo already has Trino and Superset wired as the query and dashboard surface. What is still missing is an explicit contract for how gold should be produced and served: some use cases need near real-time micro-batch updates, while others are better served by slower batch aggregation.

Without that contract, dashboard consumers have to guess which gold tables are live, which are batch-oriented, and how the semantic layer should be refreshed. This change makes the gold-to-analytics path explicit so Superset can query stable semantic views instead of raw silver tables.

## What Changes

- Add a mode-aware gold layer that can run in either `streaming` or `batch` mode against the same silver inputs.
- Define which gold tables are refreshed by micro-batch streams and which are materialized by batch jobs.
- Keep silver as the normalized source of truth and gold as the analytics/serving layer.
- Expose gold outputs through Trino semantic views so Superset reads a stable BI contract.
- Define dashboard surfaces for live operations and batch analytics with different refresh expectations and layout conventions.

## Capabilities

### New Capabilities

- `gold-processing-modes`: Gold jobs can be configured per use case to run as batch or streaming pipelines over silver inputs while producing consistent Delta outputs.
- `gold-bi-serving`: Gold outputs are exposed through Trino semantic views and Superset datasets/dashboards with explicit refresh cadence and dashboard grouping for live vs batch use cases.

### Modified Capabilities

- None.

## Impact

- Gold Spark job configuration needs to support dual execution modes and table-specific refresh behavior.
- Trino view definitions must remain aligned with the gold tables that Superset consumes.
- Superset bootstrap and dashboard layout need to reflect separate live and batch surfaces.
- Documentation and runbooks need to describe the silver -> gold -> Trino -> Superset flow and the expected refresh cadence.
