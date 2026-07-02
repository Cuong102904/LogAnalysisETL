# Spark Runtime Current

## Bronze

- Bronze vẫn là Kafka Structured Streaming -> Delta.
- Bronze giữ `raw_payload` string, `event_id`, `event_time`, `ingestion_time`, Kafka metadata, và `processing_date`.

## Silver

Silver đã được chuẩn hóa về đúng một runtime:

- entrypoint: `apps/spark/run_silver.py`
- source: Bronze Delta
- mode: Structured Streaming only
- trigger: `10 seconds`
- `maxFilesPerTrigger = 100`
- checkpoint: `silver.runtime.checkpoint` trong source profile

Không còn:

- Silver batch mode
- Silver stream mode normalize row-by-row ở driver
- `toLocalIterator()` trong hot path

## Driver vs Worker

Driver:

- load source profile
- load `routing.yaml`
- load `parsers.yaml`
- load quality rules
- compile Spark plan

Workers:

- parse `raw_payload`
- parse `context_json`
- parse `event_json`
- evaluate route conditions
- build `events_canonical`
- build domain tables
- build `silver_unknown_events`
- build `silver_invalid_events`

## Replay

- entrypoint: `apps/spark/run_silver_replay.py`
- input: unresolved `silver_unknown_events`
- output:
  - newly resolved rows -> `events_canonical` + domain tables
  - unresolved rows -> remain in unknown
  - resolved rows -> marked resolved for audit until retention cleanup
