# Responsibility-First Repository Layout

## Target Owners

- `src/learnlake/`: generic framework behavior
- `catalog/`: declarative configuration surface
- `apps/`: thin executable entrypoints
- `projects/<usecase>/`: use-case semantics and custom code
- `platform/local/`: local runtime and service assets
- `orchestration/`: schedulers and workflow runners
- `serving/`: query and BI assets
- `tests/`: verification

## Legacy Folder Migration Map

| Legacy root | New owner | Notes |
| --- | --- | --- |
| `spark/` | `platform/local/spark/`, `apps/`, `projects/daotao_ai/gold/`, `projects/daotao_ai/legacy_spark/` | Platform assets split from runtime entrypoints and use-case Gold code |
| `kafka/` | `platform/local/kafka/`, `apps/replay/`, `projects/daotao_ai/legacy_kafka/` | Kafka broker/bootstrap assets remain platform-owned; replay entrypoint is under `apps/` |
| `airflow/` | `orchestration/airflow/` | DAGs, tasks, config, plugins |
| `trino/` | `serving/trino/` | Trino config, bootstrap, and views |
| `superset/` | `serving/superset/` | Superset config and bootstrap |
| `minio/` | `platform/local/minio/` | Local object-store assets |
| `hive-metastore/` | `platform/local/hive-metastore/` | Metastore image/config |
| `deploy/scripts/` | `platform/local/scripts/` | Shared operational scripts |

## Remaining Migration Notes

- `projects/daotao_ai/legacy_spark/` is a temporary migration holding area for old Spark-specific implementation code that no longer owns supported runtime paths.
- `projects/daotao_ai/legacy_kafka/` is a temporary migration holding area for old Kafka-side Python code that is no longer part of the supported platform boundary.
- New supported runtime entrypoints are all under `apps/`.
- Workflow ordering and parallelism are declared through workflow definition YAML rather than embedded in legacy runtime folders.
