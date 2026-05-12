# MinIO Repository

MinIO la object storage cho Delta Lake:

- Delta tables under a single `lakehouse` bucket
- Streaming checkpoints and Spark event logs under `platform`

## Buckets

- `lakehouse`
- `platform`

## Bootstrap

Script tao bucket:

```bash
cd source/minio
sh scripts/make_buckets.sh
```

Chi tiet layout nam trong `bootstrap/bucket-layout.md`.
