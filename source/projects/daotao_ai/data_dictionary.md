# daotao.ai Data Dictionary

- `time`: source event time.
- `event_type`: raw platform event type.
- `name`: raw event name nếu source emit riêng với `event_type`.
- `event_source`: raw subsystem that emitted the event.
- `username`: external learner identifier when authenticated.
- `session`: source session identifier.
- `context.user_id`: numeric learner id when present.
- `context.course_id`: course context.
- `context.org_id`: organization context.
- `context.path`: request path or content path when present.
- `event`: raw payload, có thể là dict, JSON string, list, form-encoded string, hoặc invalid string.

Silver outputs:

- `silver_event_index`
  - common lineage, actor/session/course, `event_group`, `normalized_type`, `action`, `object_type`
- `silver_assessment_events`
  - `problem_id`, `assessment_action`, `attempts`, `grade`, `weighted_earned`, `answers_json`
- `silver_video_events`
  - `video_id`, `video_action`, `current_time_seconds`, `old_speed`, `new_speed`
- `silver_document_events`
  - `document_action`, `file_name`, `page_number`, `scroll_direction`, `search_query`
- `silver_navigation_events`
  - `navigation_action`, `current_url`, `target_url`, `current_tab`, `target_tab`
- `silver_exam_events`
  - `exam_action`, `exam_id`, `attempt_id`, `attempt_status`
- `silver_course_content_events`
  - `content_action`, `block_type`, `block_id`, `content_url`
- `silver_authoring_events`
  - `authoring_action`, `library_key`, `xblock_usage_key`, `request_get_json`, `request_post_json`
- `silver_auth_events`
  - `auth_action`, `provider`, `next_url`, `has_oauth_code`
- `silver_system_events`
  - `system_action`, `noise_type`, `path`, `reason`
- `silver_unknown_events`
  - unmatched raw event identity for follow-up routing
