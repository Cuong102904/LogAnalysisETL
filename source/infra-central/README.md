# infra-central repository

Repository nay tap trung quan ly local infrastructure cho toan bo he thong.

## Thanh phan phase 1

- Kafka 3 brokers (KRaft mode)
- Topic bootstrap (`kafka-init`)
- `tracking-log-replayer` (repo kafka)
- MinIO + bucket bootstrap
- Spark apps:
  - `spark-bronze-ingestor`
  - `spark-silver-transformer`
  - `spark-gold-aggregator`

## Chay stack phase 1

```bash
cd source/infra-central
docker compose -f docker-compose.phase1.yml up --build
```

## Bring-up order with Airflow orchestration

1. Start phase1 infra first:

```bash
cd source/infra-central
docker compose -f docker-compose.phase1.yml up -d --build
```

2. Start Airflow stack:

```bash
cd source/airflow
cp .env.example .env  # first time only
docker compose up -d --build
```

3. Open Airflow UI and enable DAGs:
   - `bronze_stream_health`
   - `bronze_table_maintenance`

4. Validate runtime:
   - Kafka offsets continue increasing,
   - MinIO checkpoint offsets continue increasing,
   - Bronze table file health task passes.

## Stop stack

```bash
cd source/infra-central
docker compose -f docker-compose.phase1.yml down -v
```

```bash
cd source/airflow
docker compose down -v
```

## K8s roadmap

- Khung cho Kubernetes nam trong `deploy/k8s`.
- Phase 1 chi dung Compose, phase sau moi chuyen manifest/helm.
