## ADDED Requirements

### Requirement: Canonical LearningEvent fields
The system SHALL define `LearningEvent` with required fields `event_id`, `source_id`, `source_type`, `event_time`, `raw_event_type`, `action`, `object_type`, `event_category`, `learning_relevance`, `is_authenticated`, `is_bot`, `quality_status`, `quality_errors`, `raw_event_ref`, and `processing_time`, plus optional fields `actor_id`, `actor_external_id`, `session_id`, `course_id`, `org_id`, `event_source`, `object_id`, and `context`.

#### Scenario: Silver output has canonical fields
- **WHEN** daotao.ai Bronze records are normalized
- **THEN** every output row in `silver_learning_events` contains the required `LearningEvent` fields

### Requirement: Canonical Silver is semantic not raw detail
The system SHALL use `silver_learning_events` as the canonical semantic layer and MUST preserve full raw details in Bronze rather than duplicating all source-specific details in Silver.

#### Scenario: Raw payload stays in Bronze
- **WHEN** a daotao.ai event contains source-specific nested payload fields
- **THEN** the full payload remains available through `raw_event_ref` in Bronze while Silver stores normalized semantic fields

### Requirement: Learning relevance classification
The system SHALL classify each canonical event with `learning_relevance` as one of `learning`, `non_learning`, `noise`, or `unknown`.

#### Scenario: Bot or crawler event is marked noise
- **WHEN** a daotao.ai event is classified as bot, crawler, or platform noise by catalog rules
- **THEN** the Silver event has `learning_relevance = noise` and is not counted as an active learner event
