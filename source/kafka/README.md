# Kafka repository

Repository nay tap trung vao canonical raw ingress va replay du lieu tracking log.

## Thu muc chinh

```text
source/kafka/
  config/
    topics.yaml
    producer_filter.yaml
  scripts/
    create_topics.sh
  src/
    adapters/
      mooc_tracking_log_adapter.py
    common/
      actor_identity.py
      core_producer.py
      encoding.py
      kafka_factories.py
      time_utils.py
    models/
      replay_record.py
    filters/
      mooc_event_filter.py
    producers/
      tracking_log_replayer.py
    consumers/
      __init__.py
  tests/
    integration/
    test_mooc_event_filter.py
  Dockerfile
  Dockerfile.app
  pyproject.toml
  uv.lock
```

## Topic contract

- `mooc.raw.events`: raw lines that pass the allowlist and have `username` or `context.user_id`.
- `mooc.raw.anonymous.events`: allowlisted rows missing both identifiers (same JSON value encoding as raw).
- `mooc.dlq.events`: failed events. DLQ payload keeps `raw_event`, `raw_value`, `raw`, and `event_snapshot` for quick inspection.

## Chay local nhanh

1) Khoi dong Kafka brokers trong compose goc:

```bash
cd source
docker compose up -d broker1 broker2 broker3 kafka-ui kafka-init
```

2) Tao topics:

```bash
cd source
docker compose logs -f kafka-init
```

3) Replay tracking logs:

```bash
cd source
uv sync
uv run python -m kafka.src.producers.tracking_log_replayer --brokers localhost:9092,localhost:9093,localhost:9094 --input-root ../BK_activity_logs_unzipped --topic mooc.raw.events
```

Them tuy chon toc do replay theo event-time:

```bash
uv run python -m kafka.src.producers.tracking_log_replayer --brokers localhost:9092,localhost:9093,localhost:9094 --input-root ../BK_activity_logs_unzipped --topic mooc.raw.events --speed 100.0
```

- `--speed 100.0`: nhanh hon 100 lan so voi khoang cach thoi gian goc.
- Replay su dung moc thoi gian tu event hop le dau tien, sau do map timeline event vao dong ho chay hien tai.
- Mặc định replayer đọc toàn bộ file; chỉ thêm `--max-files` khi muốn giới hạn dataset.
- Producer duoc allowlist theo `kafka/config/producer_filter.yaml`. Su kien khong thuoc allowlist duoc publish vao `mooc.dlq.events` voi `error_type=filtered_out` va co `event_snapshot` de nhan dien event.

## Integration test

```bash
cd source
uv run python -m unittest discover -s kafka/tests/integration -p "test_*.py"
```
