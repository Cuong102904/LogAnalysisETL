#!/usr/bin/env bash
set -euo pipefail

/opt/bitnami/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1024m}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-1g}" \
  --name "${GOLD_APP_NAME:-gold_aggregator}" \
  --conf spark.executorEnv.PYTHONPATH=/opt/project/src:/opt/project \
  --conf "spark.ui.port=${SPARK_UI_PORT:-4042}" \
  --conf spark.cores.max=1 \
  /opt/project/apps/spark/run_gold_dashboard_stream.py
