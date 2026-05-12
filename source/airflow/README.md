# Airflow Repository

Airflow is the orchestration layer for local Bronze operations.

## What is implemented

- Runtime compose stack is provided by `source/docker-compose.yaml`.
- Real Bronze DAGs in `dags/bronze/`:
  - `bronze_stream_health`: Kafka offset progress + checkpoint progress + file health.
  - `bronze_table_maintenance`: submits Delta OPTIMIZE/VACUUM as a Spark Standalone job + post-health-check.
- Runtime task module in `tasks/bronze/runtime_checks.py`.
- Reusable Spark wrapper task in `tasks/spark_task.py` for Bronze/Silver/Gold reuse.
- Airflow variable adapter in `config/variables.py` for shared secrets/endpoints.

## Project structure

- `dags/bronze/`, `dags/silver/`, `dags/gold/`: one DAG per file by domain.
- `tasks/`: reusable execution task factories (`spark_task`) and domain checks.
- `utils/`: shared helper utilities.
- `config/`: shared runtime variables loaded from Airflow Variables.

## Setup

1. Create the single root env file from template:

```bash
cd source
cp .env.example .env
```

2. Fill required secrets in `source/.env`:
   - `FERNET_KEY`
   - `AIRFLOW__API_AUTH__JWT_SECRET`
   - `_AIRFLOW_WWW_USER_PASSWORD`

3. Start Airflow:

```bash
cd source
docker compose up -d --build airflow
```

4. Open UI at [http://localhost:8089](http://localhost:8089).

## DAG behavior

- `bronze_stream_health` (default every 10 minutes):
  - verifies Kafka topic offsets continue increasing,
  - verifies Bronze checkpoint offsets continue increasing,
  - checks Bronze file count/small-file ratio/checkpoint staleness.
- `bronze_table_maintenance` (default every 6 hours):
  - runs Delta compaction (`OPTIMIZE`) through `spark-submit`,
  - can optionally run `VACUUM` through `spark-submit` if enabled by env,
  - reruns file health checks after maintenance.

## Notes

- All runtime knobs are in one file: `source/.env`.
- The DAGs target Bronze only. Silver and Gold orchestration remain out of scope in this phase.
