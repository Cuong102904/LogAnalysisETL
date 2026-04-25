#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${1:-broker1:29092}"

create_topic() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  docker compose exec broker1 kafka-topics \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 3 \
    --bootstrap-server "${BOOTSTRAP_SERVER}"

  docker compose exec broker1 kafka-configs \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --entity-type topics \
    --entity-name "${topic}" \
    --alter \
    --add-config "cleanup.policy=delete,retention.ms=${retention_ms},min.insync.replicas=2"
}

create_topic "mooc.raw.events" 12 1209600000
create_topic "mooc.dlq.events" 6 1209600000

for topic in \
  "mooc.raw.events" \
  "mooc.dlq.events"
do
  docker compose exec broker1 kafka-topics \
    --describe \
    --topic "${topic}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}"
done
