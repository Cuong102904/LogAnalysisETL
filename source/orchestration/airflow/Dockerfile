FROM apache/airflow:3.2.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Install providers first (apache-airflow-providers-apache-spark pulls in pyspark-client>=4.0
# which conflicts with our Spark 3.5.6 cluster). Remove pyspark-client after, then
# force-reinstall the full pyspark 3.5.6 distribution (includes spark-submit binary and
# the py4j-based Python API that matches the cluster version).
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-amazon \
    apache-airflow-providers-postgres \
    boto3 \
    delta-spark==3.2.0 \
    kafka-python-ng \
    psycopg2-binary && \
    pip uninstall -y pyspark-client && \
    pip install --no-cache-dir "pyspark==3.5.6"

ENV SPARK_APP_PATH=/opt/project \
    SPARK_CONF_DIR=/opt/project/platform/local/spark/conf \
    PYTHONPATH=/opt/airflow:/opt/project:/opt/project/src

COPY --chown=airflow:0 dags /opt/airflow/dags
COPY --chown=airflow:0 tasks /opt/airflow/tasks
COPY --chown=airflow:0 config /opt/airflow/config
COPY --chown=airflow:0 utils /opt/airflow/utils
