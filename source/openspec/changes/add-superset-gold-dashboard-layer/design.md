## Context

The silver layer already holds normalized, analytics-ready event data, with `time` as the canonical event-time. The current gold implementation is split across two styles already: a continuous Spark streaming pipeline for aggregated signals, and a separate batch vertical slice for a single summary metric. What is missing is an explicit contract that says which gold outputs are streaming, which are batch, and how those outputs are exposed to Trino and Superset.

On the serving side, Trino already reads Delta tables from MinIO through Hive Metastore and exposes semantic views for BI. Superset already boots from code, but its current dashboard bootstrap is a single flat dashboard with a fixed layout and no meaningful auto-refresh. The change needs to make the gold -> Trino -> Superset path explicit and predictable.

## Goals / Non-Goals

**Goals:**
- Define a gold layer that can be configured per output as either `streaming` or `batch`.
- Preserve silver as the only input to gold, with canonical event-time semantics.
- Expose gold outputs through Trino semantic views instead of binding Superset directly to physical tables.
- Split BI into separate live and batch surfaces with different layout and refresh expectations.
- Keep the dashboard contract code-driven so the stack can be rebuilt from repository state.

**Non-Goals:**
- Do not change bronze ingestion, silver normalization, or the event classification rules.
- Do not introduce a push-based dashboard update mechanism.
- Do not replace Trino, Hive Metastore, or Superset.
- Do not redesign all existing analytics metrics; this change only defines the serving contract.

## Use Case Map

Gold tables are the physical Delta outputs. Trino views are the semantic contract that Superset reads. The table below makes the serving boundary explicit.

| Surface | Use case | Silver inputs | Spark transform | Gold output | Trino view | Mode |
| --- | --- | --- | --- | --- | --- | --- |
| Live Ops | Video friction and watch-drop monitoring | `silver_video_events` | `build_video_anomaly_features(video_df, bucket_seconds=5)` with rolling anomaly scoring over event buckets | `video_friction_signals` | `video_friction_view` | `streaming` |
| Live Ops | Exam integrity and security monitoring | `silver_exam_events`, `silver_system_events` | `build_exam_anomaly_features(exam_df, system_df)` plus `filter_exam_security_events` and rolling anomaly scoring | `exam_integrity_signals` | `exam_anomaly_view` | `streaming` |
| Live Ops | Cross-domain anomaly triage and alert feed | `silver_video_events`, `silver_document_events`, `silver_assessment_events`, `silver_navigation_events`, `silver_course_content_events` | `build_behavior_anomalies(...)` then `build_alert_events(...)` | `behavior_anomaly_signals`, `anomaly_alerts` | `behavior_anomaly_view`, `alert_events_view` | `streaming` |
| Learning Analytics | PDF reading and document engagement | `silver_document_events` | `build_pdf_behavior_features(pdf_df)` | `pdf_engagement_features` | `pdf_engagement_view` | `batch` |
| Learning Analytics | Quiz attempt quality and difficulty | `silver_assessment_events` | `build_quiz_performance_features(performance_df)` | `quiz_attempt_metrics` | `quiz_difficulty_view` | `batch` |
| Learning Analytics | Learner journey, health, and course improvement | `silver_event_index` plus the normalized learning-event feed used by the repo's learning slice | `build_learning_journey_features(learning_df)` then Trino composition over daily profiles and anomaly signals | `user_learning_profile_daily` | `learner_health_view`, `course_improvement_view` | `batch` |

Notes:
- `silver_event_index` is the shared canonical join point for lineage, quality, and event-time semantics when a gold slice needs cross-domain context.
- Where the semantic layer still uses a legacy compatibility alias, treat it as the same domain slice; the gold contract is semantic-domain driven, not hard-coded to one physical table spelling.
- The learning-profile slice still has a legacy-compatible `silver.learning_events` naming path in the repository; design-wise it is treated as the normalized learning feed backing `user_learning_profile_daily`.
- The live alert feed is not a separate dashboard source. It is a Trino view over `anomaly_alerts`, which is produced from `behavior_anomaly_signals`.

## Dashboard Layout

### Live Ops

- Goal: one dashboard for operational monitoring, split into multiple sections or tabs.
- Layout:
  - Section 1: KPI strip for active alerts, anomaly count, worst z-score, impacted courses.
  - Section 2: short trend charts for video friction, exam anomalies, and alert volume.
  - Section 3: alert table and top offending entities.
  - Section 4: optional tab for per-domain drill-down charts when an operator needs more detail.
- Filters:
  - Keep filters minimal and fast: course, date, and anomaly severity.
  - Use Superset native filters rather than custom dashboard logic.
- Refresh:
  - Auto-refresh interval should be around 30 seconds.
  - Operators can manually refresh the dashboard on demand when they need a faster update.
  - The fastest end-to-end path is Spark micro-batch trigger -> Delta commit -> Trino query -> Superset refresh.

### Learning Analytics

- Goal: slower, wider dashboard for course-level and learner-level analysis.
- Layout:
  - Left rail or top filter bar: course, date range, content type, learner segment.
  - Main grid: course improvement overview, learner health table, PDF engagement, quiz difficulty.
  - Drill-down area: detailed tables for course or learner investigation.
- Filters:
  - More filters are acceptable because the dashboard is not expected to update every few seconds.
- Refresh:
  - Manual refresh only.
  - Batch tables are expected to update after the scheduled Spark job completes, and the user can click refresh when needed.

## Decisions

1. Use a dual-mode gold contract rather than one mixed execution model.
   - `streaming` mode is for low-latency signals that benefit from micro-batch updates.
   - `batch` mode is for slower aggregates that should be recomputed on a longer cadence.
   - A shared gold domain layer can still be reused, but execution is split by mode.
   - Alternative considered: one gold stream with a single trigger for every table. Rejected because it couples unrelated SLAs and forces expensive tables to refresh too often.

2. Read silver as the only upstream source for gold.
   - Gold consumes silver Delta tables, not Kafka, bronze, or raw logs.
   - This keeps the gold contract aligned with normalized event-time semantics and avoids re-parsing or duplicating classification logic.
   - Alternative considered: compute some dashboard metrics directly from bronze for freshness. Rejected because it weakens the semantic boundary and makes BI results harder to reason about.

3. Keep Trino semantic views as the BI contract.
   - Superset datasets should point to `delta.mooc.*_view` objects rather than physical gold tables.
   - This keeps the dashboard stable even if the underlying gold tables or aggregations change.
   - The current semantic layer already follows this pattern with views such as `video_friction_view`, `course_improvement_view`, `learner_health_view`, `pdf_engagement_view`, `quiz_difficulty_view`, `exam_anomaly_view`, `behavior_anomaly_view`, and `alert_events_view`.
   - Alternative considered: expose raw gold tables directly to Superset. Rejected because every chart would become tightly coupled to storage layout and table internals.

4. Split Superset into separate dashboard surfaces instead of one overloaded board.
   - Live surface: one `Live Ops` dashboard with multiple sections or tabs for near real-time monitoring.
   - Batch surface: `Learning Analytics` for slower trend and profiling analysis.
   - The current bootstrap can evolve from one dashboard definition into a dashboard registry keyed by surface.
   - Live dashboard layout should stay compact inside each section: KPI strip, short trend charts, and a live alert table.
   - Batch dashboard layout should be wider: filters, course-level trends, learner profiling tables, and drill-down charts.
   - Alternative considered: one dashboard with one refresh interval. Rejected because batch charts do not need the same refresh cadence as live charts and would unnecessarily load Trino.

5. Make refresh pull-based and dashboard-specific.
   - Gold jobs do not push updates to Superset.
   - Live dashboard auto-refresh should be light, around 30 seconds, and still allow manual refresh on demand.
   - Batch dashboards should refresh manually after the scheduled job completes.
   - The current Superset bootstrap already stores dashboard metadata with `refresh_frequency: 0`; this change needs a per-surface override so live dashboards can refresh faster.
   - Alternative considered: build a notification path from gold commits to BI. Rejected because it adds complexity without improving the SQL-serving model the repo already uses.

6. Classify gold outputs by cadence.
   - Streaming-first use cases: live video friction, exam integrity monitoring, and behavior anomaly alerting.
   - Batch-first use cases: PDF engagement, quiz difficulty, learner health, and course improvement analysis.
   - Cross-table semantic views such as `course_improvement_view` and `learner_health_view` remain in Trino, where they can join and summarize the gold layer without changing physical storage.
   - This split matches the actual dashboard intent in the repo: live friction/anomaly monitoring versus slower learning analytics and profiling.

7. Orchestrate streaming and batch differently.
   - Streaming gold should remain a Spark Structured Streaming job with short trigger intervals and Delta checkpoints on MinIO.
   - Batch gold should be a scheduled Spark job, suitable for Airflow or manual `spark-submit`, and should materialize partitioned Delta outputs in a reproducible way.
   - Alternative considered: run batch tables inside the same always-on stream. Rejected because batch tables have different recomputation semantics and do not benefit from always-on micro-batching.

## Risks / Trade-offs

- Short refresh intervals can overload Trino or Superset → keep live dashboards narrow, use pre-aggregated views, and limit auto-refresh to the live surface.
- Batch and streaming outputs may briefly disagree because of late-arriving events → use canonical event-time, document expected staleness, and keep the same filtering keys across surfaces.
- More dashboards and views increase bootstrap and maintenance complexity → keep the contract code-driven and generate dashboards from named surface definitions.
- Schema drift in gold tables can break views and dashboards → keep the Trino views as the only BI contract and update them alongside gold schema changes.

## Migration Plan

1. Keep the current gold streaming path as the baseline so existing behavior remains available.
2. Introduce an explicit table cadence registry that marks each gold output as `streaming` or `batch`.
3. Add or split gold execution paths so streaming tables continue to run continuously while batch tables are materialized on a longer schedule.
4. Update Trino views so they continue to expose a stable semantic layer over the gold outputs.
5. Extend Superset bootstrap from one dashboard into separate live and batch dashboard surfaces with different chart sets and refresh settings.
6. Validate query latency and dashboard load with the existing MinIO/Trino/Superset stack before widening the surface set.

Rollback strategy:
- Disable the batch schedule first if there is a freshness or correctness issue.
- Keep the streaming gold path and existing semantic views as the fallback serving path.
- Revert dashboard refresh settings to manual if Trino query load becomes too high.
