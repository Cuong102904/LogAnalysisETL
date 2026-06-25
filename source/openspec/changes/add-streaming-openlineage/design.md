## Context

LearnLake hiện chạy theo mô hình streaming-first: raw activity logs được replay vào Kafka, sau đó Spark Bronze đọc Kafka và ghi Delta Bronze, Spark Silver đọc Bronze và ghi các bảng Silver, còn Gold tạo ra các bảng phân tích/tổng hợp. Flow này không đi qua Airflow trong execution path chính, nên lineage cần được gắn trực tiếp vào các Spark entrypoints.

Repo hiện đã có kiến trúc runtime khá rõ:
- `apps/spark/run_bronze.py` và `apps/spark/run_silver.py` là streaming jobs.
- `apps/spark/run_gold.py` đang là batch-style job trên đầu vào Silver.
- `src/learnlake/runtime/spark.py` là điểm khởi tạo SparkSession chung.
- `platform/local/spark/conf/spark-defaults.conf` và `docker-compose.yaml` là nơi hợp lý để gắn cấu hình runtime.

Mục tiêu của thay đổi này là quan sát được data lineage ở mức job/dataset xuyên suốt pipeline streaming, không phải thay thế Spark UI, Kafka UI, hay một APM/trace system.

Codebase surface cho rollout đầu tiên sẽ tập trung vào:
- Spark runtime helper dùng chung cho OpenLineage config.
- Spark streaming entrypoints Bronze/Silver.
- Local compose stack cho backend/UI lineage.
- Docs vận hành local để giải thích cách bật và xem lineage.

Các file dự kiến chạm vào:
- `src/learnlake/runtime/spark.py`
- `platform/local/spark/conf/spark-defaults.conf`
- `platform/local/spark/Dockerfile` nếu cần bake thêm agent/jars
- `docker-compose.yaml`
- `apps/spark/run_bronze.py`
- `apps/spark/run_silver.py`
- `docs/local-setup.md`
- `README.md`
- thêm helper mới cho OpenLineage nếu runtime chung cần tách riêng khỏi `spark.py`

## Goals / Non-Goals

**Goals:**
- Emit OpenLineage cho các Spark jobs trong pipeline streaming.
- Track lineage từ Kafka raw topic đến Bronze, từ Bronze đến Silver.
- Provide a local lineage UI/backend so the graph can be inspected during development.
- Use a shared helper/config path so Bronze and Silver jobs inherit consistent OpenLineage settings.

**Non-Goals:**
- Do not redesign the ETL business logic or table contracts.
- Do not introduce Airflow as part of the lineage path.
- Do not attempt per-row tracing or full runtime observability across containers.
- Do not require MinIO/Hive/Superset to emit lineage because they are storage/query services, not transformation boundaries in this flow.
- Do not instrument `replay_to_kafka.py` in the initial rollout.
- Do not include Gold in the initial lineage rollout.

## Decisions

### 1. Attach lineage at the Spark job boundary, not in shared storage services

The primary instrumentation points are the Spark jobs that actually transform data:
- Bronze streaming job: Kafka -> Bronze Delta
- Silver streaming job: Bronze Delta -> Silver Delta

Rationale: OpenLineage is most useful at data boundary transitions. MinIO is only the storage backend for Delta, and Hive/Superset are consumers or metadata/query layers. Instrumenting them would add noise without improving the pipeline graph.

Alternatives considered:
- **Instrument MinIO/Hive/Superset**: rejected because it would not describe the ETL flow itself.
- **Instrument only the replay script**: rejected for this phase because the goal is to keep the first rollout focused on streaming transformations.

### 2. Use Marquez as the local backend/UI

Marquez is the preferred local UI because it gives a concrete lineage graph and run history with the OpenLineage protocol. It is a practical default for development and validation.

The Marquez service SHOULD be resource-capped in local compose because it is an additional always-on service and can increase the memory footprint of the dev stack. The first rollout should define explicit memory limits for the backend/UI container so lineage does not destabilize Spark or the rest of the stack.

Alternatives considered:
- **Console/log transport only**: useful for smoke tests, but not enough for graph inspection.
- **A custom lightweight UI**: would add work without replacing Marquez’s lineage graph capabilities.

### 3. Centralize Spark OpenLineage config in the Spark runtime

The baseline config should live in the Spark runtime layer so every Spark job gets the same namespace, transport, and listener defaults. The likely control points are:
- `platform/local/spark/conf/spark-defaults.conf`
- `src/learnlake/runtime/spark.py`
- `docker-compose.yaml` service env/args for streaming containers

Rationale: both the long-running Spark containers and the local `spark-submit` invocations need to behave consistently. Centralizing the config reduces drift between bronze/silver/gold jobs.

Alternatives considered:
- **Hardcode config per script**: rejected because it repeats the same OpenLineage wiring across entrypoints.
- **Only use compose overrides**: rejected because library code and standalone execution paths would diverge.

### 4. Defer replay-to-Kafka and Gold instrumentation

The replay script is not part of the initial rollout, and Gold is intentionally deferred. The first implementation should validate the lineage path that matters most operationally for this repository: Kafka -> Bronze -> Silver.

Rationale: this keeps the scope small enough to validate the OpenLineage plumbing without blocking on custom producer instrumentation or the Gold execution model.

Alternatives considered:
- **Instrument replay immediately**: would expand scope and require custom event emission before the core Spark path is validated.
- **Force Gold into the first rollout**: would mix a batch-oriented layer into a streaming-focused rollout and add unnecessary uncertainty.

### 5. Provide a shared helper for OpenLineage configuration

The implementation should expose a shared helper or shared configuration path so Spark Bronze and Silver jobs receive the same OpenLineage namespace, transport, and listener settings without duplicating wiring in each entrypoint.

Rationale: both Bronze and Silver are Spark entrypoints with different runtime wiring, so a shared helper reduces drift and keeps the lineage contract consistent.

Alternatives considered:
- **Duplicate config in each job**: rejected because it increases drift and makes future changes error-prone.
- **Only use compose env overrides**: rejected because library code and standalone execution paths would diverge.

## Risks / Trade-offs

- [Graph starts at Kafka] → Accept this as the initial scope and document replay instrumentation as a follow-up.
- [Silver writes multiple target tables in one streaming batch] → Accept job-level lineage first; if needed later, refine dataset naming or split outputs into clearer logical jobs.
- [Listener/config mismatch between local Spark and compose-launched Spark] → Centralize the default config and keep compose overrides minimal.
- [Marquez adds another local service] → Make it optional in local development profiles if the full graph is not required for every run.
- [Marquez consumes extra RAM] → Set explicit memory limits for the backend/UI container and keep it behind a local profile or opt-in service.
- [Gold is deferred] → Keep the design modular so Gold can be added later without changing Bronze/Silver lineage contracts.

## Migration Plan

1. Add Marquez as the local lineage backend/UI in the compose stack.
2. Add Spark OpenLineage defaults to the shared Spark runtime.
3. Ensure Bronze and Silver streaming jobs inherit the same namespace and transport settings.
4. Validate the graph in Marquez using Kafka -> Bronze -> Silver streaming runs.
5. If the graph is too coarse, refine naming and dataset boundaries before expanding scope.

Rollback strategy:
- Remove the Marquez service from compose.
- Remove OpenLineage listener/transport defaults from Spark config.
- Leave the ETL jobs unchanged so the pipeline still runs without lineage.
