# Overall Architecture

Pipeline target: replay/file input -> Kafka raw topic -> LearnLake Bronze/Silver/Gold apps -> MinIO-backed Delta -> Trino/Superset serving.

```mermaid
flowchart LR
    dataFiles[BK activity logs] --> replayer[apps/replay/replay_to_kafka.py]
    replayer --> kafkaRaw[Kafka raw topic]
    kafkaRaw --> bronzeApp[apps/spark/run_bronze.py]
    bronzeApp --> bronzeDelta[Delta bronze_events]
    bronzeDelta --> silverApp[apps/spark/run_silver.py]
    bronzeDelta --> replayApp[apps/spark/run_silver_replay.py]
    silverApp --> silverDelta[Delta events_canonical + domain tables + unknown/invalid]
    replayApp --> silverDelta
    silverDelta --> goldApp[apps/spark/run_gold.py]
```

## Silver Boundary

- Bronze preserves raw payloads.
- Silver owns semantic parsing, classification, quarantine, and replay.
- Gold must not reconstruct behavior by reparsing raw Bronze payloads.

## Silver Runtime

- One runtime only for Bronze -> Silver normalization:
  - Structured Streaming
  - `trigger(processingTime='10 seconds')`
  - `maxFilesPerTrigger=100`
- Replay is a separate bounded job for `silver_unknown_events`, not a second normalization runtime.
