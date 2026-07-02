# Event Flow

## End-to-End Flow

1. `tracking_log_replayer` đọc JSON lines và publish raw events vào Kafka.
2. Bronze app đọc Kafka, giữ `raw_payload` string, enrich metadata kỹ thuật, và ghi `bronze_events`.
3. Silver app đọc `bronze_events` bằng đúng một Structured Streaming runtime:
   - `trigger(processingTime='10 seconds')`
   - `maxFilesPerTrigger=100`
4. Driver chỉ load source profile, `routing.yaml`, `parsers.yaml`, quality rules, rồi compile Spark plan.
5. Workers parse `raw_json`, `context_json`, `event_json`, attach route metadata, và build:
   - `events_canonical`
   - domain tables
   - `silver_unknown_events`
   - `silver_invalid_events`
6. `run_silver_replay.py` là bounded manual replay path cho `silver_unknown_events`.
7. Gold đọc từ `events_canonical` và các domain tables phù hợp.

## Silver Outcomes

- Match route + pass validation:
  - ghi `events_canonical`
  - ghi domain tables tương ứng
- Không match route:
  - chỉ ghi `silver_unknown_events`
- Match route nhưng fail parse/schema/quality:
  - chỉ ghi `silver_invalid_events`

## Unknown Strategy

- `silver_unknown_events` giữ `raw_json` và metadata đủ để replay.
- Unknown event không còn emit canonical row.
- Khi thêm route/parser mới, operator chạy replay bounded job trên unknown unresolved.
- Unknown resolved vẫn được giữ lại ở trạng thái audit cho tới khi retention hoặc `VACUUM` dọn dữ liệu cũ.
