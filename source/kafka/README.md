# Kafka repository

Repository nay tap trung vao canonical raw ingress va replay du lieu tracking log.

## Thu muc chinh

```text
source/kafka/
  config/
    topics.yaml
  schemas/
    json/
      mooc_raw_envelope.schema.json
      mooc_tracking_event.schema.json
      mooc_dlq.schema.json
  scripts/
    create_topics.sh
  src/
    common.py
    producers/
      simulator.py
      tracking_log_replayer.py
    legacy/
      simulator.py
    consumers/
      validator_router.py
  tests/
    integration/
  docker-compose.yml
  Dockerfile
  Dockerfile.app
  pyproject.toml
  uv.lock
```

## Topic contract

- `mooc.raw.events`: canonical raw ingest stream.
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
cd source/kafka
uv sync
uv run python -m src.producers.tracking_log_replayer --brokers localhost:9092,localhost:9093,localhost:9094 --input-root ../BK_activity_logs_unzipped --topic mooc.raw.events
```

Them tuy chon toc do replay theo event-time:

```bash
uv run python -m src.producers.tracking_log_replayer --brokers localhost:9092,localhost:9093,localhost:9094 --input-root ../BK_activity_logs_unzipped --topic mooc.raw.events --speed 2.0
```

- `--speed 1.0`: phat theo khoang cach thoi gian goc cua truong `time`.
- `--speed 2.0`: nhanh gap doi so voi khoang cach thoi gian goc.
- Replay su dung moc thoi gian tu event hop le dau tien, sau do map timeline event vao dong ho chay hien tai.

4) Validator router duoc giu o che do deprecated de backward compatibility.

## Integration test

```bash
cd source/kafka
uv run python -m unittest discover -s tests/integration -p "test_*.py"
```

