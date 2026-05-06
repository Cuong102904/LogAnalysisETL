FROM bitnamilegacy/spark:3.5 

USER root

# Keep local Spark image extensible for connectors/jars in CI/CD.
COPY conf /opt/bitnami/spark/conf
COPY apps /opt/project/spark/apps
COPY domain /opt/project/spark/domain
COPY infrastructure /opt/project/spark/infrastructure
COPY configs /opt/project/spark/configs
COPY utils /opt/project/spark/utils
COPY src /opt/project/spark/src

# Ensure Python runtime has Spark and YAML deps for app modules.
RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    pyyaml>=6.0.0

WORKDIR /opt/project/spark
ENV PYTHONPATH=/opt/project/spark

USER 1001
