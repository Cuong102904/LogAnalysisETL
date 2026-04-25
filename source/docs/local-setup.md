# Local Setup

## 1. Start infrastructure

```bash
cd source/infra-central
docker compose -f docker-compose.phase1.yml up --build
```

## 2. Replay dữ liệu thật vào Kafka raw topic

```bash
cd source/kafka
uv run python -m src.producers.tracking_log_replayer \
  --brokers broker1:29092,broker2:29092,broker3:29092 \
  --topic mooc.raw.events \
  --input-root ../BK_activity_logs_unzipped \
  --max-files 50
```

## 3. Run Spark apps

```bash
cd source/spark
uv run python -m apps.bronze_ingestor.main
uv run python -m apps.silver_transformer.main
uv run python -m apps.gold_aggregator.main
```

## 4. Verify Delta data in MinIO

- Buckets: `bronze`, `silver`, `gold`, `checkpoints`.
- Kiểm tra path theo `spark/configs/storage/minio.yaml`.
