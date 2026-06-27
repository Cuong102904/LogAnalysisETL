#!/usr/bin/env bash
set -euo pipefail

/opt/bitnami/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1024m}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-1g}" \
  --name "${GOLD_BATCH_APP_NAME:-gold_dashboard_serving_batch}" \
  --conf spark.executorEnv.PYTHONPATH=/opt/project/src:/opt/project \
  --conf "spark.ui.port=${SPARK_UI_PORT:-4044}" \
  --conf spark.cores.max=1 \
  /opt/project/apps/spark/run_gold_dashboard_batch.py
