FROM apache/hive:4.0.0

USER root

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar /opt/hive/lib/postgres.jar

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar /tmp/hadoop-aws-3.3.6.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /tmp/aws-java-sdk-bundle-1.12.262.jar

RUN apt-get update     && apt-get install -y --no-install-recommends gettext-base netcat-openbsd     && rm -rf /var/lib/apt/lists/*

ENV HADOOP_HEAPSIZE=256
ENV HADOOP_HEAPSIZE_MAX=256

RUN set -eux;     install -D /tmp/hadoop-aws-3.3.6.jar /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.3.6.jar;     install -D /tmp/hadoop-aws-3.3.6.jar /opt/hive/lib/hadoop-aws-3.3.6.jar;     install -D /tmp/aws-java-sdk-bundle-1.12.262.jar /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.12.262.jar;     install -D /tmp/aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar;     rm -f /tmp/hadoop-aws-3.3.6.jar /tmp/aws-java-sdk-bundle-1.12.262.jar

COPY core-site.xml.template /opt/templates/core-site.xml.template
COPY metastore-site.xml.template /opt/templates/metastore-site.xml.template
COPY entrypoint.sh /opt/hive/bin/entrypoint-metastore.sh

RUN sed -i 's/-Xmx1G/-Xmx128m/' /entrypoint.sh

RUN mkdir -p /opt/hive/data/warehouse     && chmod +x /opt/hive/bin/entrypoint-metastore.sh     && chown -R hive:hive /opt/hive/data/warehouse /opt/hive/lib/postgres.jar /opt/templates /opt/hive/bin/entrypoint-metastore.sh

USER hive
