# Kafka Platform Assets

`platform/local/kafka/` now contains only local broker/bootstrap assets for the repository runtime.

## Contents

- `Dockerfile`: Kafka broker image for the local compose stack
- `config/topics.yaml`: topic bootstrap definitions
- `config/producer_filter.yaml`: allowlist/filter configuration used by the local raw-ingest contract
- `scripts/create_topics.sh`: topic bootstrap script used by `kafka-init`

## Supported Runtime Boundary

- Broker/bootstrap assets stay here under `platform/local/kafka/`.
- Supported replay entrypoint lives under `apps/replay/replay_to_kafka.py`.
- Legacy Kafka Python producer/filter code has been moved to `projects/daotao_ai/legacy_kafka/` as migration reference and is no longer the supported runtime surface.
