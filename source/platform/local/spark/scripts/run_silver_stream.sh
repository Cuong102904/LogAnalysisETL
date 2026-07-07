#!/usr/bin/env bash
set -euo pipefail

/opt/bitnami/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-3g}" \
  --name "${SILVER_APP_NAME:-learnlake_silver_normalization}" \
  --conf spark.executorEnv.PYTHONPATH=/opt/project/src:/opt/project \
  --conf "spark.ui.port=${SPARK_UI_PORT:-4041}" \
  --conf spark.cores.max=4 \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.sql.shuffle.partitions=4 \
  /opt/project/apps/spark/run_silver.py \
  --source daotao_ai
