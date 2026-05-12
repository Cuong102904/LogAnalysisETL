#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-${1:-broker1:29092}}"

run_kafka_topics() {
  if command -v kafka-topics >/dev/null 2>&1; then
    kafka-topics "$@"
  elif command -v docker >/dev/null 2>&1; then
    docker compose exec broker1 kafka-topics "$@"
  else
    echo "kafka-topics is not available" >&2
    exit 1
  fi
}

run_kafka_configs() {
  if command -v kafka-configs >/dev/null 2>&1; then
    kafka-configs "$@"
  elif command -v docker >/dev/null 2>&1; then
    docker compose exec broker1 kafka-configs "$@"
  else
    echo "kafka-configs is not available" >&2
    exit 1
  fi
}

create_topic() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  run_kafka_topics \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 3 \
    --bootstrap-server "${BOOTSTRAP_SERVER}"

  run_kafka_configs \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --entity-type topics \
    --entity-name "${topic}" \
    --alter \
    --add-config "cleanup.policy=delete,retention.ms=${retention_ms},min.insync.replicas=2"
}

create_topic "mooc.raw.events" 12 1209600000
create_topic "mooc.raw.anonymous.events" 12 1209600000
create_topic "mooc.dlq.events" 6 1209600000

for topic in \
  "mooc.raw.events" \
  "mooc.raw.anonymous.events" \
  "mooc.dlq.events"
do
  run_kafka_topics \
    --describe \
    --topic "${topic}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}"
done
