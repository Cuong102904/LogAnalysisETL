# MinIO Setup Guide - Phase 1

## Overview

MinIO là object storage tương thích S3, dùng để lưu trữ dữ liệu Bronze/Silver/Gold layer trong pipeline. Phase 1 cần tạo 4 buckets: `bronze`, `silver`, `gold`, `checkpoints`.

## Điều kiện tiên quyết

MinIO service đã chạy trong docker-compose phase1:

```bash
docker compose -f docker-compose.phase1.yml ps minio
```

Nếu chưa, khởi động:

```bash
docker compose -f docker-compose.phase1.yml up -d minio
```

## Cách 1: Dùng MinIO CLI (mc) - Lệnh

### 1.1 Tạo alias kết nối

```bash
docker exec mooc-streaming-phase1-minio-1 \
  mc alias set local http://localhost:9000 minio minio123456
```

**Giải thích:**
- `local` - tên alias trong mc
- `http://localhost:9000` - endpoint MinIO (trong container, dùng localhost)
- `minio` - access key (default)
- `minio123456` - secret key (default)

### 1.2 Tạo buckets

```bash
docker exec mooc-streaming-phase1-minio-1 mc mb --ignore-existing local/bronze
docker exec mooc-streaming-phase1-minio-1 mc mb --ignore-existing local/silver
docker exec mooc-streaming-phase1-minio-1 mc mb --ignore-existing local/gold
docker exec mooc-streaming-phase1-minio-1 mc mb --ignore-existing local/checkpoints
```

**Hoặc một lệnh duy nhất:**

```bash
docker exec mooc-streaming-phase1-minio-1 sh -c '\
  mc alias set local http://localhost:9000 minio minio123456 && \
  mc mb --ignore-existing local/bronze && \
  mc mb --ignore-existing local/silver && \
  mc mb --ignore-existing local/gold && \
  mc mb --ignore-existing local/checkpoints && \
  mc ls local'
```

### 1.3 Kiểm tra buckets đã tạo

```bash
docker exec mooc-streaming-phase1-minio-1 mc ls local
```

Output sẽ hiện:

```
[2026-05-05 07:02:26 UTC]     0B bronze/
[2026-05-05 07:02:26 UTC]     0B checkpoints/
[2026-05-05 07:02:26 UTC]     0B gold/
[2026-05-05 07:02:26 UTC]     0B silver/
```

---

## Cách 2: Dùng MinIO Web UI

### 2.1 Truy cập Web Console

1. Mở browser: `http://localhost:9001`
2. Đăng nhập:
   - **Username**: `minio`
   - **Password**: `minio123456`

### 2.2 Tạo Access Key (tùy chọn)

Nếu muốn dùng credentials khác:

1. Vào **Access Keys** (menu trái)
2. Click **Create access key**
3. Copy `Access Key` và `Secret Key`
4. Lưu vào `.env` file (nếu có)

### 2.3 Tạo Buckets

1. Vào **Object Browser** hoặc **Buckets** tab
2. Click **Create bucket**
3. Nhập tên: `bronze`, `silver`, `gold`, `checkpoints` (tạo lần lượt)
4. Click **Create bucket**

---

## Kiểm tra bucket từ Spark

Khi Spark Bronze Ingestor chạy, nó sẽ ghi dữ liệu vào bucket. Kiểm tra:

```bash
# List objects trong bronze bucket
docker exec mooc-streaming-phase1-minio-1 mc ls --recursive local/bronze

# Watch realtime
docker exec mooc-streaming-phase1-minio-1 mc watch local/bronze
```

---

## Troubleshooting

### "The specified bucket does not exist"

**Nguyên nhân:** Spark container cache lỗi cũ hoặc bucket chưa tạo.

**Giải pháp:**
1. Kiểm tra bucket tồn tại: `mc ls local`
2. Nếu chưa, tạo như hướng dẫn trên
3. Xóa Spark container cũ và restart:
   ```bash
   docker compose -f docker-compose.phase1.yml down spark-bronze-ingestor
   docker compose -f docker-compose.phase1.yml up -d spark-bronze-ingestor
   ```

### "Connection refused"

**Nguyên nhân:** MinIO chưa chạy hoặc endpoint sai.

**Giải pháp:**
```bash
# Kiểm tra MinIO chạy
docker compose -f docker-compose.phase1.yml ps minio

# Nếu down, restart
docker compose -f docker-compose.phase1.yml up -d minio
```

---

## Default Credentials

- **Endpoint**: `http://minio:9000` (từ trong container), `http://localhost:9000` (từ host)
- **Access Key**: `minio`
- **Secret Key**: `minio123456`
- **Console Port**: `9001` (web UI)
- **API Port**: `9000` (S3-compatible)

Nếu thay đổi, update `source/.env` và restart services.

---

## Next Steps

Sau khi bucket sẵn sàng:
1. Chạy Kafka + tracking-log-replayer để đưa dữ liệu vào Kafka
2. Chạy Bronze Ingestor để đọc từ Kafka và ghi vào MinIO
3. Kiểm tra dữ liệu trong MinIO Web UI hoặc mc command
