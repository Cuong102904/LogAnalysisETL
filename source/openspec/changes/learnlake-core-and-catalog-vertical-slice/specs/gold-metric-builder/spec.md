## ADDED Requirements

### Requirement: Gold reads canonical Silver
The system SHALL build first-phase Gold metrics from `silver_learning_events` and MUST NOT parse daotao.ai raw payloads directly.

#### Scenario: Gold metric does not read Bronze raw payload
- **WHEN** `gold_course_activity_summary` is built
- **THEN** it reads canonical Silver fields and does not depend on daotao.ai raw payload structure

### Requirement: Course activity summary metric
The system SHALL implement `gold_course_activity_summary` with `source_id`, `course_id`, `window_start`, `window_end`, `active_learners`, `learning_event_count`, `noise_event_count`, `unknown_event_count`, and `total_event_count`.

#### Scenario: Course activity is aggregated
- **WHEN** Silver contains learning, noise, and unknown events for a course window
- **THEN** the Gold metric aggregates active learners and event counts for that source, course, and window

### Requirement: Metric uses catalog definition
The system SHALL load metric configuration from `catalog/metrics/gold_course_activity_summary.yaml`.

#### Scenario: Gold app loads metric profile
- **WHEN** `run_gold.py --source daotao_ai --metric gold_course_activity_summary` is executed
- **THEN** the app loads metric configuration from the catalog before calling the metric builder
