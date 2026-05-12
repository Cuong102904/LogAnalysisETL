#!/usr/bin/env sh
set -eu

MC_ALIAS="${MC_ALIAS:-local}"
MC_ENDPOINT="${MC_ENDPOINT:-http://minio:9000}"
MC_ACCESS_KEY="${MINIO_ROOT_USER:-minio}"
MC_SECRET_KEY="${MINIO_ROOT_PASSWORD:-minio123456}"

mc alias set "${MC_ALIAS}" "${MC_ENDPOINT}" "${MC_ACCESS_KEY}" "${MC_SECRET_KEY}"
mc mb --ignore-existing "${MC_ALIAS}/lakehouse"
mc mb --ignore-existing "${MC_ALIAS}/platform"
