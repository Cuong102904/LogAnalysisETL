# daotao.ai Mapping Notes

Source-specific routing semantics are declared in
[routing.yaml](/home/cuong/Desktop/DATN/source/projects/daotao_ai/routing.yaml).

Common `silver_event_index` mapping is declared in
[catalog/mappings/daotao_ai_to_learning_event.yaml](/home/cuong/Desktop/DATN/source/catalog/mappings/daotao_ai_to_learning_event.yaml).

Mapping assumptions:

- `time` là source of truth cho `event_time`.
- `event`, `name`, `event_type`, `event_source`, `context.path`, và `host` cùng tham gia routing.
- Open edX usage/block identifiers được extract từ `context.path` hoặc raw event path.
- Browser video actions được chuẩn hóa thành `play`, `pause`, `load`, `stop`, `seek`, `speed_change`.
- Unknown routes vẫn emit `silver_event_index` và `silver_unknown_events`.
- OAuth code/token không được copy nguyên vào Silver domain facts; chỉ giữ signal như `provider` hoặc `has_oauth_code`.

Approved daotao extractors hiện có:

- `daotao_assessment_event`
- `daotao_video_event`
- `daotao_video_completion_event`
- `daotao_document_event`
- `daotao_navigation_event`
- `daotao_exam_event`
- `daotao_course_content_event`
- `daotao_authoring_event`
- `daotao_auth_event`
- `daotao_system_event`
- `daotao_unknown_event`
