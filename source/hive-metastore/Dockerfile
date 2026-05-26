FROM apache/hive:4.0.0

USER root

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar /opt/hive/lib/postgres.jar

RUN mkdir -p /opt/hive/data/warehouse \
    && chown -R hive:hive /opt/hive/data/warehouse /opt/hive/lib/postgres.jar

USER hive
