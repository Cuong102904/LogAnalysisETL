FROM bitnamilegacy/spark:3.5 

USER root

# Keep local Spark image extensible for connectors/jars in CI/CD.
COPY platform/local/spark/conf /opt/bitnami/spark/conf
COPY apps /opt/project/apps
COPY catalog /opt/project/catalog
# `orchestration` directory is not present in this workspace (contains Airflow/DAGs).
# Omit copying it to avoid Docker build errors. Add it back when orchestration
# assets (e.g. Airflow) are available or needed in the image.
# COPY orchestration /opt/project/orchestration
COPY platform /opt/project/platform
COPY projects /opt/project/projects
COPY serving /opt/project/serving
COPY src /opt/project/src
COPY tests/fixtures /opt/project/tests/fixtures

ENV SPARK_APP_PATH=/opt/project \
    SPARK_CONF_DIR=/opt/bitnami/spark/conf \
    PYTHONPATH=/opt/project/src:/opt/project \
    PYSPARK_PYTHON=/opt/bitnami/python/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python

# Ensure Python runtime has Spark and YAML deps for app modules.
RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    "confluent-kafka>=2.3.0" \
    "pydantic>=2.0.0" \
    "pyyaml>=6.0.0"

RUN /opt/bitnami/python/bin/python -c "from pathlib import Path; from urllib.request import urlretrieve; jar_dir = Path('/opt/bitnami/spark/jars'); artifacts = {'delta-spark_2.12-3.2.0.jar': 'https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar', 'delta-storage-3.2.0.jar': 'https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar', 'antlr4-runtime-4.9.3.jar': 'https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar', 'spark-sql-kafka-0-10_2.12-3.5.1.jar': 'https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar', 'spark-token-provider-kafka-0-10_2.12-3.5.1.jar': 'https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar', 'kafka-clients-3.4.1.jar': 'https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar', 'commons-pool2-2.11.1.jar': 'https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar', 'lz4-java-1.8.0.jar': 'https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar', 'snappy-java-1.1.10.3.jar': 'https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.3/snappy-java-1.1.10.3.jar', 'slf4j-api-2.0.7.jar': 'https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.7/slf4j-api-2.0.7.jar', 'hadoop-client-runtime-3.3.4.jar': 'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar', 'hadoop-client-api-3.3.4.jar': 'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar', 'hadoop-aws-3.3.4.jar': 'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar', 'commons-logging-1.1.3.jar': 'https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar', 'jsr305-3.0.0.jar': 'https://repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar', 'aws-java-sdk-bundle-1.12.262.jar': 'https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar', 'wildfly-openssl-1.0.7.Final.jar': 'https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar'}; [urlretrieve(url, jar_dir / file_name) for file_name, url in artifacts.items() if not (jar_dir / file_name).exists()]"

RUN mkdir -p /opt/bitnami/spark/.ivy2 /tmp/spark-events \
    && ln -sf /opt/bitnami/python/bin/python /usr/local/bin/python3 \
    && ln -sf /opt/bitnami/python/bin/python /usr/local/bin/python \
    && chown -R 1001:0 /opt/project /opt/bitnami/spark/.ivy2 /tmp/spark-events

WORKDIR /opt/project

USER 1001
