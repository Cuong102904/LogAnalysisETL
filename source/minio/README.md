# MinIO Repository

MinIO la object storage cho Delta Lake:

- Bronze tables
- Silver tables
- Gold tables
- Streaming checkpoints

## Buckets

- `bronze`
- `silver`
- `gold`
- `checkpoints`

## Bootstrap

Script tao bucket:

```bash
cd source/minio
sh scripts/make_buckets.sh
```

Chi tiet layout nam trong `bootstrap/bucket-layout.md`.

