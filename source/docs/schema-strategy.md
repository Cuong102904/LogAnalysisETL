# Schema Strategy

## Raw Strategy

- Kafka payload giữ nguyên JSON line từ source tracking log.
- Không ép schema cứng ở ingest stage để tránh mất dữ liệu do drift.

## Bronze Strategy

- Bronze giữ `value_raw` string + metadata.
- Parse tối thiểu để đánh dấu `parse_status`:
  - `ok`
  - `invalid_json`
  - `missing_required`

## Silver Strategy

- Silver không còn là một bảng wide duy nhất.
- Framework chuẩn hóa theo 2 lớp:
  - `silver_event_index`: one row per valid normalized source event
  - domain fact tables: `silver_assessment_events`, `silver_video_events`,
    `silver_document_events`, `silver_navigation_events`,
    `silver_exam_events`, `silver_course_content_events`,
    `silver_authoring_events`, `silver_auth_events`,
    `silver_system_events`, `silver_unknown_events`
- `event` có thể là object, JSON string, list, form-encoded string, hoặc invalid string;
  normalize qua parsed-payload step trước khi route.
- Routing đọc rule từ source-pack YAML, không hard-code Open edX event names trong `src/learnlake/`.
- `silver_invalid_events` giữ các record fail contract hoặc fail required quality rules.

## Gold Strategy

- Gold chỉ dùng cột đã chuẩn hóa từ `silver_event_index` và domain fact tables.
- Feature schema có version để backward-compatible khi thêm metrics mới.

## Downstream Compatibility

- LearnLake Gold vertical slice hiện đọc `silver_event_index` cho metric `gold_course_activity_summary`.
- Legacy Spark migration reference hiện nằm dưới `projects/daotao_ai/legacy_spark/`, còn semantic layer phục vụ BI nằm dưới `serving/trino/`.
- Việc migrate toàn bộ legacy Spark Silver tables, Trino views, notebooks, và Superset datasets sang LearnLake multi-target Silver được ghi nhận là deferred migration ngoài phạm vi change này.
