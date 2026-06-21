# LearnLake Source

This repository is organized around responsibility boundaries, not technology-owned top-level folders.

## Repository Layout

- `src/learnlake/`: framework core for contracts, connectors, ingestion, normalization, quality, workflow planning, and runtime helpers.
- `catalog/`: declarative source profiles, mappings, event types, quality rules, metrics, and workflow definitions.
- `apps/`: thin executable entrypoints for Bronze, Silver, Gold, replay, maintenance, and task execution.
- `projects/`: use-case packs. `projects/daotao_ai/` owns daotao.ai semantics, transforms, workflow notes, and use-case-specific Gold code.
- `platform/local/`: local runtime assets for Kafka, Spark, MinIO, Hive Metastore, and shared platform scripts.
- `orchestration/`: scheduler/runtime orchestration assets. Airflow now lives under `orchestration/airflow/`.
- `serving/`: query and BI assets. Trino and Superset now live under `serving/`.
- `tests/`: contract, unit, integration, and fixture coverage.

## Migration Map

The previous technology-first folders have been absorbed into explicit owners:

- `spark/` -> `platform/local/spark/` for image/conf, `apps/` for runnable entrypoints, `projects/daotao_ai/gold/` for use-case-specific Gold logic, `projects/daotao_ai/legacy_spark/` for temporary migration reference code.
- `kafka/` -> `platform/local/kafka/` for broker/bootstrap assets, `apps/replay/` for supported replay entrypoints, and `projects/daotao_ai/legacy_kafka/` for migration reference code.
- `airflow/` -> `orchestration/airflow/`.
- `trino/` -> `serving/trino/`.
- `superset/` -> `serving/superset/`.
- `minio/` -> `platform/local/minio/`.
- `hive-metastore/` -> `platform/local/hive-metastore/`.
- `deploy/scripts/` -> `platform/local/scripts/`.

## Supported Runtime Paths

- Bronze: `apps/spark/run_bronze.py`
- Silver: `apps/spark/run_silver.py`
- Gold metric builder: `apps/spark/run_gold.py`
- Workflow task runner: `apps/spark/run_task.py`
- Replay: `apps/replay/replay_to_kafka.py`
- Delta maintenance: `apps/maintenance/delta_maintenance.py`

## Workflow Definitions

Use-case execution order is declared in workflow specs instead of hard-coded runtime folders.

- Current workflow definition: [catalog/workflows/daotao_ai_pipeline.yaml](/home/cuong/Desktop/DATN/source/catalog/workflows/daotao_ai_pipeline.yaml)
- Current source profile: [catalog/sources/daotao_ai.yaml](/home/cuong/Desktop/DATN/source/catalog/sources/daotao_ai.yaml)

## Local Stack

`docker-compose.yaml` remains the top-level entrypoint for the local stack, but it now builds and mounts assets from `platform/local/`, `orchestration/`, and `serving/`.

Primary UIs:

- Kafka UI: `http://localhost:8085`
- Spark Master: `http://localhost:8081`
- Spark History Server: `http://localhost:18080`
- Airflow: `http://localhost:8089`
- MinIO: `http://localhost:9001`
- Trino: `http://localhost:8080`
- Superset: `http://localhost:8088`
