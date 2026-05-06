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
  docker-compose.yml
  Dockerfile
  Dockerfile.app
  pyproject.toml
  uv.lock
```

## Topic contract

- `mooc.raw.events`: raw lines that pass the allowlist and have `username` or `context.user_id`.
- `mooc.raw.anonymous.events`: allowlisted rows missing both identifiers (same JSON value encoding as raw).
- `mooc.dlq.events`: failed events.

## Chay local nhanh

1) Khoi dong Kafka brokers:

```bash
cd source/kafka
docker compose up -d
```

2) Tao topics:

```bash
cd source/kafka
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh
```

3) Replay tracking logs:

```bash
cd source
uv sync
uv run python -m kafka.src.producers.tracking_log_replayer --brokers localhost:9092,localhost:9093,localhost:9094 --input-root ../BK_activity_logs_unzipped --topic mooc.raw.events
```

Them tuy chon toc do replay theo event-time:

```bash
uv run python -m kafka.src.producers.tracking_log_replayer --brokers localhost:9092,localhost:9093,localhost:9094 --input-root ../BK_activity_logs_unzipped --topic mooc.raw.events --speed 2.0
```

- `--speed 1.0`: phat theo khoang cach thoi gian goc cua truong `time`.
- `--speed 2.0`: nhanh gap doi so voi khoang cach thoi gian goc.
- Replay su dung moc thoi gian tu event hop le dau tien, sau do map timeline event vao dong ho chay hien tai.
- Producer duoc allowlist theo `kafka/config/producer_filter.yaml`. Su kien khong thuoc allowlist duoc publish vao `mooc.dlq.events` voi `error_type=filtered_out`.

## Integration test

```bash
cd source
uv run python -m unittest discover -s kafka/tests/integration -p "test_*.py"
```

