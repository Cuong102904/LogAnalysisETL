FROM bitnamilegacy/spark:3.5 

USER root

# Keep local Spark image extensible for connectors/jars in CI/CD.
COPY conf /opt/bitnami/spark/conf
COPY apps /opt/project/spark/apps
COPY domain /opt/project/spark/domain
COPY infrastructure /opt/project/spark/infrastructure
COPY configs /opt/project/spark/configs
COPY utils /opt/project/spark/utils

ENV SPARK_APP_PATH=/opt/project/spark \
    SPARK_CONF_DIR=/opt/bitnami/spark/conf \
    PYTHONPATH=/opt/project/spark \
    SPARK_JARS_PACKAGES=io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    PYSPARK_PYTHON=/opt/bitnami/python/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python

# Ensure Python runtime has Spark and YAML deps for app modules.
RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    "pyyaml>=6.0.0"

RUN /opt/bitnami/python/bin/python -c "from pathlib import Path; from urllib.request import urlretrieve; jar_dir = Path('/opt/bitnami/spark/jars'); artifacts = {'hadoop-aws-3.3.4.jar': 'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar', 'aws-java-sdk-bundle-1.12.262.jar': 'https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar'}; [urlretrieve(url, jar_dir / file_name) for file_name, url in artifacts.items() if not (jar_dir / file_name).exists()]"

RUN mkdir -p /opt/bitnami/spark/.ivy2 /tmp/spark-events \
    && ln -sf /opt/bitnami/python/bin/python /usr/local/bin/python3 \
    && ln -sf /opt/bitnami/python/bin/python /usr/local/bin/python \
    && chown -R 1001:0 /opt/project /opt/bitnami/spark/.ivy2 /tmp/spark-events

WORKDIR /opt/project/spark

USER 1001
