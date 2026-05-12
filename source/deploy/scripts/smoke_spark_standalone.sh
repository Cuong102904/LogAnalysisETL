#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT_DIR}/../docker-compose.yaml}"
COMPOSE_DIR="$(dirname "${COMPOSE_FILE}")"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

compose() {
  docker compose --project-directory "${COMPOSE_DIR}" -f "${COMPOSE_FILE}" "$@"
}

echo "Checking Spark Master UI"
curl -fsS "http://localhost:8081" >/dev/null

echo "Checking Spark workers registered"
curl -fsS "http://localhost:8081/json/" | grep -q '"workers"'

echo "Running Spark Standalone + S3A + Delta smoke job"
compose exec -T spark-master bash -lc "cat >/tmp/smoke_s3_delta.py <<'PY'
from pyspark.sql.functions import lit

from infrastructure.spark.session import build_spark

spark = build_spark('spark_standalone_s3_delta_smoke')
assert spark.sparkContext.master.startswith('spark://'), spark.sparkContext.master
path = 's3a://lakehouse/smoke/spark_standalone_delta'
spark.range(1).withColumn('status', lit('ok')).write.format('delta').mode('overwrite').save(path)
assert spark.read.format('delta').load(path).count() == 1
spark.stop()
PY
/opt/bitnami/spark/bin/spark-submit --master '${SPARK_MASTER_URL}' --deploy-mode client \
  --conf spark.executorEnv.PYTHONPATH=/opt/project/spark \
  /tmp/smoke_s3_delta.py"

echo "Producing one Kafka sample event"
printf '{"event_type":"smoke","username":"smoke-user","time":"2026-01-01T00:00:00Z","context":{}}\n' \
  | compose exec -T broker1 kafka-console-producer \
      --bootstrap-server broker1:29092 \
      --topic "${KAFKA_TOPIC_RAW:-mooc.raw.events}"

echo "Checking Bronze checkpoint prefix exists or waits for stream creation"
compose exec -T minio sh -lc \
  "mc alias set local http://minio:9000 '${MINIO_ROOT_USER:-minio}' '${MINIO_ROOT_PASSWORD:-minio123456}' >/dev/null &&
   mc ls 'local/${MINIO_BUCKET_PLATFORM:-platform}/mooc/bronze_ingestor' || true"

echo "Smoke checks submitted. Verify Bronze rows and History Server in the UIs."
