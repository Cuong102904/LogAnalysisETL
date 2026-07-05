#!/usr/bin/env bash
set -euo pipefail

/opt/bitnami/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --driver-memory "${SPARK_DRIVER_MEMORY:-768m}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-768m}" \
  --name "${GOLD_ASSESSMENT_BATCH_APP_NAME:-gold_assessment_batch}" \
  --conf spark.executorEnv.PYTHONPATH=/opt/project/src:/opt/project \
  --conf "spark.ui.port=${SPARK_UI_PORT:-4044}" \
  --conf "spark.cores.max=${SPARK_CORES_MAX:-2}" \
  --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES:-1}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES:-1}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --conf "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM:-2}" \
  /opt/project/apps/spark/run_gold_assessment_batch.py "$@"
