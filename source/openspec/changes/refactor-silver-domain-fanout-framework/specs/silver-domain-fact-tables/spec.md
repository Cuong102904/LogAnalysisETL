## ADDED Requirements

### Requirement: Typed domain fact tables
The system SHALL support typed Silver domain fact tables for analytics-ready projections of normalized learning-log events, including assessment, video, document/PDF, navigation, exam, course content, authoring, auth, system/noise, and unknown event domains.

#### Scenario: Assessment event creates assessment fact
- **WHEN** a normalized event is routed to the assessment domain
- **THEN** the system writes a typed row to `silver_assessment_events` with assessment-specific fields and the same `event_id` as `silver_event_index`

#### Scenario: Video event creates video fact
- **WHEN** a normalized event is routed to the video domain
- **THEN** the system writes a typed row to `silver_video_events` with video-specific fields and the same `event_id` as `silver_event_index`

#### Scenario: Non-domain event does not create unrelated fact
- **WHEN** a normalized auth event is processed
- **THEN** the system does not write unrelated assessment, video, document, navigation, or exam fact rows for that event

### Requirement: Assessment fact schema
The `silver_assessment_events` table SHALL include typed fields for assessment analysis: `event_id`, `event_time`, `actor_id`, `session_id`, `course_id`, `org_id`, `problem_id`, `assessment_action`, `problem_display_name`, `response_type`, `input_type`, `attempts`, `success`, `grade`, `max_grade`, `weighted_earned`, `weighted_possible`, `answers_json`, `correct_map_json`, `submission_json`, and `event_transaction_id`.

#### Scenario: Grade persisted fields are extracted
- **WHEN** a daotao.ai `edx.grades.problem.submitted` event is normalized
- **THEN** the assessment fact row contains `problem_id`, `weighted_earned`, `weighted_possible`, and `event_transaction_id` when present in the payload

#### Scenario: Server problem check fields are extracted
- **WHEN** a daotao.ai server `problem_check` event is normalized
- **THEN** the assessment fact row contains `attempts`, `success`, `grade`, `max_grade`, `answers_json`, `correct_map_json`, and `submission_json` when present in the payload

### Requirement: Video fact schema
The `silver_video_events` table SHALL include typed fields for video analysis: `event_id`, `event_time`, `actor_id`, `session_id`, `course_id`, `org_id`, `video_id`, `video_action`, `video_code`, `duration_seconds`, `current_time_seconds`, `old_time_seconds`, `new_time_seconds`, `old_speed`, `new_speed`, `saved_position`, `transcript_language`, and `completion_status`.

#### Scenario: Browser video payload is extracted
- **WHEN** a daotao.ai browser `play_video`, `pause_video`, `stop_video`, `load_video`, `seek_video`, or `speed_change_video` event is normalized
- **THEN** the video fact row contains video id, action, code, duration, and available playback position or speed fields

#### Scenario: Server video state save is extracted
- **WHEN** a daotao.ai `/type@video/.../save_user_state` event is normalized
- **THEN** the video fact row contains `video_action = save_position`, the parsed video block id, and the saved video position when present

### Requirement: Document fact schema
The `silver_document_events` table SHALL include typed fields for PDF/book/document analysis: `event_id`, `event_time`, `actor_id`, `session_id`, `course_id`, `org_id`, `document_action`, `document_type`, `document_id`, `asset_url`, `file_name`, `chapter`, `chapter_title`, `page_number`, `old_value`, `new_value`, `zoom_amount`, `scroll_direction`, `search_query`, `search_status`, `case_sensitive`, and `highlight_all`.

#### Scenario: PDF scroll and zoom fields are extracted
- **WHEN** daotao.ai `textbook.pdf.page.scrolled` or `textbook.pdf.display.scaled` events are normalized
- **THEN** the document fact row contains the PDF action and available page, chapter, scroll direction, or zoom amount fields

#### Scenario: PDF book server page load is extracted
- **WHEN** a daotao.ai `/courses/.../pdfbook/...` server route is normalized
- **THEN** the document fact row records a document load action with course and URL-derived document context where available

### Requirement: Navigation and course content fact schemas
The system SHALL separate learner navigation/UI facts from server course-content page facts so link clicks and sequence widget events are not confused with content page loads.

#### Scenario: Link click is navigation fact
- **WHEN** a daotao.ai `edx.ui.lms.link_clicked` browser event is normalized
- **THEN** the navigation fact row contains `current_url`, `target_url`, and `navigation_action = link_clicked`

#### Scenario: Courseware page is course content fact
- **WHEN** a daotao.ai `/courses/.../courseware/...` server route is normalized
- **THEN** the course content fact row records a courseware page or jump action with parsed course/block context where available

### Requirement: Exam fact schema
The `silver_exam_events` table SHALL include typed fields for timed/proctored exam analysis: `event_id`, `event_time`, `actor_id`, `session_id`, `course_id`, `org_id`, `exam_action`, `exam_id`, `exam_content_id`, `exam_name`, `exam_default_time_limit_mins`, `exam_is_proctored`, `exam_is_practice_exam`, `exam_is_active`, `attempt_id`, `attempt_user_id`, `attempt_started_at`, `attempt_completed_at`, `attempt_status`, `attempt_elapsed_time_secs`, and `quiz_nav_action`.

#### Scenario: Timed exam lifecycle fields are extracted
- **WHEN** a daotao.ai `edx.special_exam.timed.*` event is normalized
- **THEN** the exam fact row contains exam metadata and attempt state fields present in the payload

#### Scenario: Proctoring poll is represented
- **WHEN** a daotao.ai `/api/edx_proctoring/...` event is normalized
- **THEN** the exam fact row records a proctoring API action without requiring raw API payload parsing beyond configured fields

### Requirement: Domain facts are joinable to event index
Every domain fact row SHALL include `event_id` and SHALL be joinable to exactly one `silver_event_index` row for the same normalized source event.

#### Scenario: Fact row joins to event index
- **WHEN** a Silver consumer joins a domain fact table to `silver_event_index` on `event_id`
- **THEN** each fact row has one matching event-index row with common metadata and quality state
