## ADDED Requirements

### Requirement: Source-agnostic Silver event index
The system SHALL write a `silver_event_index` table containing one source-agnostic index row for each valid normalized source event, independent of whether the event also creates domain fact rows.

#### Scenario: Valid daotao event creates event index row
- **WHEN** a valid daotao.ai Bronze event is normalized
- **THEN** the system writes exactly one corresponding row to `silver_event_index`

#### Scenario: Domain event still creates event index row
- **WHEN** a daotao.ai video, assessment, PDF, navigation, exam, authoring, auth, system, or unknown event is normalized
- **THEN** the system writes the common event metadata to `silver_event_index` before or alongside any domain fact row

### Requirement: Event index common fields
The `silver_event_index` table SHALL include common fields required for lineage, filtering, normalized semantics, and joins: `event_id`, `source_id`, `source_type`, `raw_event_ref`, `event_time`, `ingest_time`, `processing_time`, `actor_id`, `actor_external_id`, `session_id`, `course_id`, `org_id`, `raw_name`, `raw_event_type`, `event_source`, `event_group`, `normalized_type`, `action`, `object_type`, `object_id`, `path`, `page`, `referer`, `host`, `ip`, `user_agent`, `is_authenticated`, `is_bot`, `is_noise`, `quality_status`, `quality_errors`, `payload_kind`, and optional `payload_json`.

#### Scenario: Event index exposes normalized semantics
- **WHEN** a raw event such as daotao.ai `play_video` is normalized
- **THEN** the event index row includes `event_group`, `normalized_type`, `action`, and `object_type` values suitable for source-agnostic analysis

#### Scenario: Event index preserves lineage
- **WHEN** a Silver consumer inspects an event index row
- **THEN** `event_id` and `raw_event_ref` allow the row to be traced back to the Bronze raw event

### Requirement: Source event time is authoritative
The system SHALL populate `silver_event_index.event_time` from the source event-time field configured by the source profile and MUST NOT infer event order from file line order, ingest order, Kafka offset order, or arrival order.

#### Scenario: Out-of-order ingest keeps source event time
- **WHEN** two Bronze events are processed in an order different from their source `time` values
- **THEN** their `silver_event_index.event_time` values reflect the source event time rather than processing order

### Requirement: Event index excludes unbounded raw detail
The system SHALL preserve full raw payloads in Bronze and SHALL NOT duplicate unbounded raw detail such as rendered HTML problem payloads into event-index columns.

#### Scenario: Problem graded HTML is not duplicated as common detail
- **WHEN** a daotao.ai `problem_graded` browser event contains rendered HTML in its raw `event` field
- **THEN** `silver_event_index` stores lineage and selected normalized metadata while the full HTML remains available through `raw_event_ref`

### Requirement: Event index quality state
The system SHALL include quality state on each event-index row and SHALL route invalid or ignored records to an invalid output instead of silently dropping them.

#### Scenario: Invalid event is traceable
- **WHEN** a Bronze event cannot produce required event-index fields such as `event_id` or `event_time`
- **THEN** the system writes an invalid output record containing quality errors and a Bronze reference
