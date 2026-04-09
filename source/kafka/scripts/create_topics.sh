#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-broker1:29092}"

topics=(
  "lsp.raw.logs"
  "lsp.canonical.events"
  "lsp.dlq"
)

for topic in "${topics[@]}"; do
  echo "Creating topic: ${topic}"
  kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions 6 \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms=$((24 * 60 * 60 * 1000))
done

for topic in "${topics[@]}"; do
  echo "Describe topic: ${topic}"
  kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --describe --topic "${topic}"
done

