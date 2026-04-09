# Streaming Platform Layout (Flat under source)

Tất cả thành phần được tách trực tiếp dưới `source/` để dễ quản lý CI/CD theo service:

- `source/kafka/`: script và config Kafka.
- `source/minio/`: ghi chú và cấu hình MinIO.
- `source/spark/`: Spark config + Spark jobs code.
- `source/airflow/`: Airflow DAGs.
- `source/trino/`: cấu hình Trino (`etc`, `catalog`).
- `source/producer/`: Python Kafka producer replay logs.
- `source/common/`: thư viện dùng chung.
- `source/deploy/`: compose entrypoint (`docker-compose.streaming.yml`) + `.env`.
- `source/docs/`: runbook và tài liệu vận hành.

Run nhanh:

```bash
cd source/deploy
cp env/.env.example .env
docker compose -f docker-compose.streaming.yml --env-file .env up -d
```

