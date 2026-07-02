# Runbook

## Silver Runtime

- Normalize stream:

```bash
cd source
docker compose up -d silver-stream
docker compose logs -f silver-stream
```

- Manual replay of unresolved unknown events:

```bash
cd source
docker compose exec -T spark-master \
  /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.executorEnv.PYTHONPATH=/opt/project/src:/opt/project \
  /opt/project/apps/spark/run_silver_replay.py --source daotao_ai
```

## Runtime Guarantees

- Silver normalize path chỉ còn một Structured Streaming runtime.
- Trigger mặc định: `10 seconds`
- `maxFilesPerTrigger`: `100`
- Unknown event không emit `events_canonical`.
- Unknown replay là bounded job riêng.

## Expected Silver Outputs

- `events_canonical`
- `problem_submissions`
- `problem_grades`
- `exam_attempts`
- `video_interactions`
- `navigation_events`
- `content_access_events`
- `system_noise_events`
- `silver_unknown_events`
- `silver_invalid_events`

## Debug Checklist

1. Bronze Delta có commit mới.
2. Silver checkpoint dưới `silver.runtime.checkpoint` tăng đều.
3. `events_canonical` có row mới với `event_time_utc` lấy từ source `time`.
4. Event không match route chỉ xuất hiện ở `silver_unknown_events`.
5. Event fail parse/schema/quality chỉ xuất hiện ở `silver_invalid_events`.
6. Replay chuyển unknown resolved sang canonical/domain mà không tạo duplicate `event_id`.

## Cleanup

- Unknown resolved records vẫn được giữ để audit.
- Cleanup vật lý có thể làm sau bằng retention policy và `VACUUM`.
