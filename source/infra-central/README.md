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

## Stop stack

```bash
cd source/infra-central
docker compose -f docker-compose.phase1.yml down -v
```

## K8s roadmap

- Khung cho Kubernetes nam trong `deploy/k8s`.
- Phase 1 chi dung Compose, phase sau moi chuyen manifest/helm.
