# Kafka producer lab (real exam-day logs)

Project này ingest trực tiếp file log JSONL thật từ `BK_activity_logs_unzipped/exam_days/...` vào Kafka local (3 broker), phân loại `exam/learning/noise` và route vào nhiều topic.

## Cấu trúc thư mục

```text
source/kafka-producer-lab/
  docker-compose.yml
  pyproject.toml
  uv.lock
  README.md
  scripts/
    create_topics.sh
  src/
    producer.py
    event_classifier.py
```

## Yêu cầu

- Docker + Docker Compose
- [uv](https://docs.astral.sh/uv/)

## 1) Khởi động Kafka cluster

```bash
cd source/kafka-producer-lab
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
cd source/kafka-producer-lab
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh
```

Retention mặc định đã set trong script:
- raw: 14 ngày
- exam: 90 ngày
- learning: 30 ngày
- noise: 3 ngày
- dlq: 14 ngày

## 3) Cài dependencies

```bash
cd source/kafka-producer-lab
uv sync
```

## 4) Chạy producer ingest từ file log thật

Ví dụ ingest file bạn đang dùng:

```bash
cd source/kafka-producer-lab
uv run python src/producer.py \
  --input-file /home/cuong/Desktop/DATN/BK_activity_logs_unzipped/exam_days/tracking.log-20260118-1768702621 \
  --brokers localhost:9092,localhost:9093,localhost:9094 \
  --key-mode course_user
```

Chạy nhanh 500 dòng đầu để test:

```bash
uv run python src/producer.py \
  --input-file /home/cuong/Desktop/DATN/BK_activity_logs_unzipped/exam_days/tracking.log-20260118-1768702621 \
  --max-records 500
```

## 5) Rule phân loại log

`src/event_classifier.py` dùng rule:
- **exam**:
  - `event_type` bắt đầu `edx.special_exam.timed.attempt.`
  - Hoặc `name/event_type/path/page` chứa `edx_proctoring` hoặc `proctored_exam`
  - Hoặc `problem_check` / `edx.grades.problem.submitted` có ngữ cảnh exam (`FinalExam`, `in_exam`)
- **learning**:
  - `play_video`, `pause_video`, `seek_video`, `speed_change_video`, `completion`, `edx.ui.lms.sequence.*`
  - Hoặc `problem_check`/`edx.grades.problem.submitted` không có tín hiệu exam
- **noise**:
  - URL scan phổ biến (`wp-login`, `wp-json`, `xmlrpc`, `/admin/`, `.env`, `.git/config`, `robots.txt`)
  - Hoặc server log không username và user-agent bot/scanner
- **other**: fallback

## 6) Partition key strategy

- `--key-mode course_user` (mặc định): key = `course_id|username`
- `--key-mode session`: key = `session`

Mỗi record được gửi:
- 1 bản vào `lms.raw.events`
- 1 bản vào topic theo class (`exam`, `learning`, `noise`, hoặc fallback `learning`)

Payload Kafka có `envelope`:
- `ingest_ts`, `source_file`, `line_no`, `classification`, `classification_reason`, `event_hash`, `payload`

## 7) Kiểm tra message ở partition nào

### Cách 1: nhìn log callback của producer

Producer in:

```text
delivered topic=... partition=... offset=... key='...'
```

### Cách 2: đọc trực tiếp bằng console consumer

```bash
cd source/kafka-producer-lab
docker compose exec broker1 kafka-console-consumer \
  --topic lms.exam.events \
  --from-beginning \
  --bootstrap-server broker1:29092 \
  --property print.key=true \
  --property print.partition=true \
  --property key.separator=" | "
```

## 8) Dừng cụm Kafka

```bash
cd source/kafka-producer-lab
docker compose down
```

Xóa luôn data volumes:

```bash
docker compose down -v
```
