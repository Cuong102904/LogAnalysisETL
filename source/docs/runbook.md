# Runbook (local)

## Start stack

```bash
cd source/deploy
cp env/.env.example .env
docker compose -f docker-compose.streaming.yml --env-file .env up -d
```

## Validate services

- Kafka: `broker1:29092` (inside docker network), `localhost:9092` (host).
- MinIO: `http://localhost:9001`
- Spark master UI: `http://localhost:8080`
- Airflow: `http://localhost:8089`
- Trino: `http://localhost:8088`

## Topics

Topics được tạo tự động bởi service `kafka-init`:

- `lsp.raw.logs`
- `lsp.canonical.events`
- `lsp.dlq`

Nếu muốn chạy lại thủ công:

```bash
cd source
BOOTSTRAP_SERVER=broker1:29092 ./kafka/scripts/create_topics.sh
```

## Producer

```bash
cd source/producer
uv sync
uv run python -m producer.main \
  --data-dir /home/cuong/Desktop/DATN/BK_activity_logs_unzipped \
  --speed 120 \
  --raw-topic lsp.raw.logs \
  --canonical-topic lsp.canonical.events \
  --dlq-topic lsp.dlq
```

## Spark job (skeleton)

Draft chạy trong Spark container:

```bash
docker exec -it lsp-spark-master bash
```

Bạn có thể copy code job vào image hoặc mount thêm volume vào compose rồi chạy `spark-submit`.

## Stop stack

```bash
cd source/deploy
docker compose -f docker-compose.streaming.yml down -v
```
