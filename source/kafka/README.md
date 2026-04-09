# Kafka infra (KRaft)

Thư mục này chỉ chứa hạ tầng Kafka local (KRaft 3 brokers) và script tạo topics.
Logic producer được tách riêng sang `source/producer`.

## Cấu trúc thư mục

```text
source/kafka/
  docker-compose.yml
  Dockerfile
  README.md
  scripts/
    create_topics.sh
```

## Yêu cầu

- Docker + Docker Compose
- [uv](https://docs.astral.sh/uv/)

## 1) Khởi động Kafka cluster

```bash
cd source/kafka
docker compose up -d
```

Host bootstrap cho Python producer chạy ngoài container: `localhost:9092,localhost:9093,localhost:9094`.

## 2) Tạo topics theo thiết kế ingest

Thiết kế topic:
- `lms.raw.events`: full raw stream để replay/audit
- `lms.exam.events`: log thi/proctoring
- `lms.learning.events`: log học bình thường
- `lms.noise.events`: bot/scan/noise
- `lms.dlq.events`: parse lỗi

Chạy script tạo topic + retention:

```bash
cd source/kafka
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh
```

Retention mặc định đã set trong script:
- raw: 14 ngày
- exam: 90 ngày
- learning: 30 ngày
- noise: 3 ngày
- dlq: 14 ngày

## 3) Chạy producer ingest từ file log thật

```bash
cd source/producer
uv sync
uv run python -m producer.main \
  --data-dir /home/cuong/Desktop/DATN/BK_activity_logs_unzipped \
  --brokers localhost:9092,localhost:9093,localhost:9094
```

Chạy nhanh với giới hạn số event để test:

```bash
uv run python -m producer.main \
  --data-dir /home/cuong/Desktop/DATN/BK_activity_logs_unzipped \
  --max-events 500
```

## 4) Kiểm tra message ở partition nào

### Cách 1: nhìn log callback của producer app

Producer in:

```text
delivered topic=... partition=... offset=... key='...'
```

### Cách 2: đọc trực tiếp bằng console consumer

```bash
cd source/kafka
docker compose exec broker1 kafka-console-consumer \
  --topic lms.exam.events \
  --from-beginning \
  --bootstrap-server broker1:29092 \
  --property print.key=true \
  --property print.partition=true \
  --property key.separator=" | "
```

## 5) Dừng cụm Kafka

```bash
cd source/kafka
docker compose down
```

Xóa luôn data volumes:

```bash
docker compose down -v
```

