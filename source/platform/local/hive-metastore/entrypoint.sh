#!/usr/bin/env bash
set -euo pipefail

: "${OBJECT_STORE_ENDPOINT:=http://minio:9000}"
: "${OBJECT_STORE_ACCESS_KEY:=minio}"
: "${OBJECT_STORE_SECRET_KEY:=minio123456}"
: "${OBJECT_STORE_PATH_STYLE_ACCESS:=true}"
: "${OBJECT_STORE_SSL_ENABLED:=false}"
: "${HIVE_METASTORE_DB_HOST:=hive-metastore-db}"
: "${HIVE_METASTORE_DB_PORT:=5432}"
: "${HIVE_METASTORE_DB_NAME:=hive_metastore}"
: "${HIVE_METASTORE_DB_USER:=hive}"
: "${HIVE_METASTORE_DB_PASSWORD:=hivepassword}"
: "${HIVE_METASTORE_PORT:=9083}"
: "${HIVE_WAREHOUSE_DIR:=s3a://lakehouse/learnlake}"

export AWS_ACCESS_KEY_ID="${OBJECT_STORE_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${OBJECT_STORE_SECRET_KEY}"
export AWS_ACCESS_KEY="${OBJECT_STORE_ACCESS_KEY}"
export AWS_SECRET_KEY="${OBJECT_STORE_SECRET_KEY}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export HIVE_METASTORE_HADOOP_OPTS=" -Dfs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem -Dfs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem -Dfs.s3a.endpoint=${OBJECT_STORE_ENDPOINT} -Dfs.s3a.path.style.access=${OBJECT_STORE_PATH_STYLE_ACCESS} -Dfs.s3a.connection.ssl.enabled=${OBJECT_STORE_SSL_ENABLED} -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider -Dfs.s3a.access.key=${OBJECT_STORE_ACCESS_KEY} -Dfs.s3a.secret.key=${OBJECT_STORE_SECRET_KEY}"

envsubst < /opt/templates/core-site.xml.template > /opt/hadoop/etc/hadoop/core-site.xml
envsubst < /opt/templates/core-site.xml.template > /opt/hive/conf/core-site.xml
envsubst < /opt/templates/metastore-site.xml.template > /opt/hive/conf/metastore-site.xml

until nc -z "${HIVE_METASTORE_DB_HOST}" "${HIVE_METASTORE_DB_PORT}"; do
  sleep 2
done

if ! /opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
  /opt/hive/bin/schematool -dbType postgres -initSchema
fi

exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service metastore
