FROM bitnami/spark:3.5.1

USER root

# Keep local Spark image extensible for connectors/jars in CI/CD.
COPY conf /opt/bitnami/spark/conf

USER 1001
