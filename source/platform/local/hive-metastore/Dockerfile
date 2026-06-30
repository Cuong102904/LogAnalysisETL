FROM apache/hive:4.0.0

USER root

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar /opt/hive/lib/postgres.jar

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar /tmp/hadoop-aws-3.3.6.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /tmp/aws-java-sdk-bundle-1.12.262.jar

ENV HIVE_METASTORE_HADOOP_OPTS=" \
    -Dfs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    -Dfs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    -Dfs.s3a.endpoint=http://minio:9000 \
    -Dfs.s3a.path.style.access=true \
    -Dfs.s3a.connection.ssl.enabled=false \
    -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    -Dfs.s3a.access.key=minio \
    -Dfs.s3a.secret.key=minio123456 \
    -Dderby.system.home=/opt/hive/data"
ENV AWS_ACCESS_KEY_ID=minio
ENV AWS_SECRET_ACCESS_KEY=minio123456
ENV AWS_ACCESS_KEY=minio
ENV AWS_SECRET_KEY=minio123456
ENV AWS_REGION=us-east-1
ENV AWS_DEFAULT_REGION=us-east-1
ENV HADOOP_HEAPSIZE=128
ENV HADOOP_HEAPSIZE_MAX=128

RUN set -eux; \
    install -D /tmp/hadoop-aws-3.3.6.jar /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.3.6.jar; \
    install -D /tmp/hadoop-aws-3.3.6.jar /opt/hive/lib/hadoop-aws-3.3.6.jar; \
    install -D /tmp/aws-java-sdk-bundle-1.12.262.jar /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.12.262.jar; \
    install -D /tmp/aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar; \
    rm -f /tmp/hadoop-aws-3.3.6.jar /tmp/aws-java-sdk-bundle-1.12.262.jar

COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY core-site.xml /opt/hive/conf/core-site.xml

RUN sed -i 's/-Xmx1G/-Xmx128m/' /entrypoint.sh

RUN mkdir -p /opt/hive/data/warehouse \
    && chown -R hive:hive /opt/hive/data/warehouse /opt/hive/lib/postgres.jar

USER hive
