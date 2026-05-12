# Local Setup

## 1. Tạo file .env

Nếu chưa có `.env`:

```bash
cd source
cp .env.example .env
```

Nếu đã có `.env` thì dùng luôn, không cần tạo lại. Kiểm tra nhanh:

```bash
ls source/.env
```

Sửa các giá trị `change-me` trong `.env` trước khi chạy (đặc biệt `MINIO_ROOT_PASSWORD`).

## 2. Khởi động toàn bộ stack

```bash
cd source
docker compose up -d --build
```

Lệnh này bật: Kafka (3 broker), Kafka UI, MinIO, Spark Master + Worker, History Server, Airflow, và Bronze stream.

Kiểm tra tất cả đang chạy:

```bash
docker compose ps
```

Các service quan trọng cần ở trạng thái `healthy` hoặc `running`:
- `lsp-broker1`, `lsp-broker2`, `lsp-broker3` — healthy
- `lsp-minio` — healthy
- `lsp-spark-master` — healthy
- `lsp-spark-worker-1` — running
- `lsp-bronze-stream` — running (driver đang idle chờ data)

`lsp-kafka-init` và `lsp-minio-init` sẽ ở trạng thái `exited (0)` sau khi hoàn tất bootstrap topic và bucket.

## 3. Truyền dữ liệu vào Kafka

### Cách A — Data thật từ BK_activity_logs_unzipped (khuyến nghị)

Folder `BK_activity_logs_unzipped` có cấu trúc:

```
BK_activity_logs_unzipped/
  normal_days/
    202510/   (các file tracking.log-*.json)
    202511/
  exam_days/
    tracking.log-20260118-*.json
    ...
```

Chạy replayer qua Docker (không cần cài thêm gì):

```bash
cd source
docker compose up -d tracking-log-replayer
docker compose logs -f tracking-log-replayer
```

Tham số mặc định trong `docker-compose.yaml`: `--max-files 10 --speed 4` (10 file đầu, replay nhanh 4x).

Để chạy nhiều file hơn hoặc điều chỉnh tốc độ, override trực tiếp:

```bash
cd source
docker compose run --rm tracking-log-replayer \
  python -m kafka.src.producers.tracking_log_replayer \
  --brokers broker1:29092,broker2:29092,broker3:29092 \
  --input-root /data/activity_logs \
  --topic mooc.raw.events \
  --anonymous-topic mooc.raw.anonymous.events \
  --max-files 20 \
  --speed 10
```

Tham số `--max-files 0` để replay toàn bộ file (chậm, chỉ dùng khi cần full dataset).

Lưu ý:
- `mooc.raw.events` chỉ nhận event thuộc allowlist của producer (xem `kafka/config/producer_filter.yaml`).
- Event bị loại hoặc lỗi decode/validation được đẩy vào `mooc.dlq.events`.
- Event có `username` hoặc `context.user_id` trống được đẩy sang `mooc.raw.anonymous.events`.

### Cách B — Data giả để smoke test nhanh

Gửi một vài message JSON thủ công qua kafka-console-producer:

```bash
docker exec -i lsp-broker1 kafka-console-producer \
  --bootstrap-server broker1:29092 \
  --topic mooc.raw.events \
  --property "parse.key=true" \
  --property "key.separator=:" << 'EOF'
user_001:{"event_type":"play_video","username":"alice","course_id":"CS101","video_id":"v001","time":"2026-05-06T09:00:00Z","currentTime":0,"duration":600}
user_002:{"event_type":"pause_video","username":"bob","course_id":"CS101","video_id":"v001","time":"2026-05-06T09:00:05Z","currentTime":120,"duration":600}
user_003:{"event_type":"problem_check","username":"carol","course_id":"CS202","problem_id":"p001","time":"2026-05-06T09:00:10Z","success":true}
user_004:{"event_type":"seq_goto","username":"dave","course_id":"CS202","time":"2026-05-06T09:00:15Z","old":1,"new":3}
user_005:{"event_type":"stop_video","username":"eve","course_id":"MATH301","video_id":"v005","time":"2026-05-06T09:00:20Z","currentTime":300}
EOF
```

Để test cả nhánh `parse_status = invalid_json`, gửi thêm 1 message không phải JSON:

```bash
docker exec -i lsp-broker1 kafka-console-producer \
  --bootstrap-server broker1:29092 \
  --topic mooc.raw.events << 'EOF'
THIS IS NOT JSON
EOF
```

## 4. Kiểm tra pipeline đang xử lý

Theo dõi log Bronze driver:

```bash
cd source
docker compose logs -f bronze-stream
```

Khi có data vào, sẽ thấy progress block dạng:

```
"batchId" : 1,
"numInputRows" : 5,
"inputRowsPerSecond" : 210.5,
```

Kiểm tra trạng thái Spark cluster:

```bash
docker compose ps bronze-stream spark-master spark-worker-1
```

## 5. Xác nhận dữ liệu đã ghi vào MinIO (Bronze Delta)

Kiểm tra file Delta đã được tạo:

```bash
docker exec lsp-minio sh -c "
  mc alias set local http://minio:9000 minio minio123456 --quiet 2>/dev/null
  mc ls --recursive local/lakehouse/mooc/bronze/mooc_events_raw/
"
```

Cần thấy:
- `_delta_log/000....json` — commit log, tăng dần theo mỗi batch.
- `ingest_date=YYYY-MM-DD/ingest_hour=HH/part-*.snappy.parquet` — data files.

Kiểm tra checkpoint tiến triển:

```bash
docker exec lsp-minio sh -c "
  mc alias set local http://minio:9000 minio minio123456 --quiet 2>/dev/null
  mc ls local/platform/mooc/bronze_ingestor/offsets/
  mc ls local/platform/mooc/bronze_ingestor/commits/
"
```

Số lượng file trong `offsets/` và `commits/` phải bằng nhau và tăng theo thời gian.

## 6. Dừng stack

Dừng nhưng giữ lại data (Kafka offset, MinIO Delta, checkpoint):

```bash
cd source
docker compose down
```

Dừng và xóa toàn bộ data (bắt đầu lại từ đầu):

```bash
cd source
docker compose down -v
```

## 7. UIs

| Port | URL | Xem gì |
|---|---|---|
| 8081 | http://localhost:8081 | Spark Master — workers, running apps |
| 8082 | http://localhost:8082 | Spark Worker 1 — executor, tasks |
| 8085 | http://localhost:8085 | Kafka UI — topics, offset, consumer lag |
| 9001 | http://localhost:9001 | MinIO Console — Delta files (user: minio) |
| 18080 | http://localhost:18080 | Spark History Server — job/stage/task history |
| 8089 | http://localhost:8089 | Airflow — maintenance DAGs |
