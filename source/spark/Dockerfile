FROM bitnami/spark:3.5.1

USER root

# Keep local Spark image extensible for connectors/jars in CI/CD.
COPY conf /opt/bitnami/spark/conf
COPY apps /opt/project/spark/apps
COPY domain /opt/project/spark/domain
COPY infrastructure /opt/project/spark/infrastructure
COPY configs /opt/project/spark/configs
COPY utils /opt/project/spark/utils
COPY src /opt/project/spark/src

USER 1001
