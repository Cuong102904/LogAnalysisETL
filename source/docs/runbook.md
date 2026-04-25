# Runbook (current local)

## 1) Start stack

```bash
cd source/infra-central
docker compose -f docker-compose.phase1.yml up --build
```

## 2) Topic inventory

- `mooc.raw.events`
- `mooc.dlq.events`

## 3) Replay dữ liệu tracking logs

```bash
cd source/kafka
uv run python -m src.producers.tracking_log_replayer \
  --brokers broker1:29092,broker2:29092,broker3:29092 \
  --topic mooc.raw.events \
  --input-root ../BK_activity_logs_unzipped
```

## 4) Run Spark apps

```bash
cd source/spark
uv run python -m apps.bronze_ingestor.main
uv run python -m apps.silver_transformer.main
uv run python -m apps.gold_aggregator.main
```

## 5) Verification checklist

- Raw topic co du lieu.
- Bronze Delta co metadata + dedup key.
- Silver co day du `learning/performance/system/unknown/video_interactions`.
- Gold co `video_anomaly_features`.

## 6) Stop stack

```bash
cd source/infra-central
docker compose -f docker-compose.phase1.yml down -v
```
