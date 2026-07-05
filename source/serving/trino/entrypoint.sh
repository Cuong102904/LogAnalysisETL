#!/usr/bin/env bash
set -euo pipefail

: "${TRINO_HIVE_METASTORE_URI:=thrift://hive-metastore:9083}"
: "${TRINO_S3_ENDPOINT:=http://minio:9000}"
: "${TRINO_S3_REGION:=us-east-1}"
: "${TRINO_S3_PATH_STYLE_ACCESS:=true}"
: "${TRINO_S3_ACCESS_KEY:=minio}"
: "${TRINO_S3_SECRET_KEY:=minio123456}"

export AWS_ACCESS_KEY_ID="${TRINO_S3_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${TRINO_S3_SECRET_KEY}"
export AWS_ACCESS_KEY="${TRINO_S3_ACCESS_KEY}"
export AWS_SECRET_KEY="${TRINO_S3_SECRET_KEY}"
export AWS_REGION="${TRINO_S3_REGION}"
export AWS_DEFAULT_REGION="${TRINO_S3_REGION}"
export HADOOP_CONF_DIR=/etc/trino

cat > /etc/trino/catalog/hive.properties <<EOF
connector.name=hive
hive.non-managed-table-writes-enabled=true
hive.metastore=thrift
hive.metastore.uri=${TRINO_HIVE_METASTORE_URI}
fs.native-s3.enabled=true
s3.endpoint=${TRINO_S3_ENDPOINT}
s3.region=${TRINO_S3_REGION}
s3.path-style-access=${TRINO_S3_PATH_STYLE_ACCESS}
s3.aws-access-key=${TRINO_S3_ACCESS_KEY}
s3.aws-secret-key=${TRINO_S3_SECRET_KEY}
s3.max-connections=500
EOF

cat > /etc/trino/catalog/delta.properties <<EOF
connector.name=delta_lake
hive.metastore=thrift
hive.metastore.uri=${TRINO_HIVE_METASTORE_URI}
fs.native-s3.enabled=true
s3.endpoint=${TRINO_S3_ENDPOINT}
s3.region=${TRINO_S3_REGION}
s3.path-style-access=${TRINO_S3_PATH_STYLE_ACCESS}
s3.aws-access-key=${TRINO_S3_ACCESS_KEY}
s3.aws-secret-key=${TRINO_S3_SECRET_KEY}
s3.max-connections=500
delta.register-table-procedure.enabled=true
EOF

exec /usr/lib/trino/bin/run-trino
