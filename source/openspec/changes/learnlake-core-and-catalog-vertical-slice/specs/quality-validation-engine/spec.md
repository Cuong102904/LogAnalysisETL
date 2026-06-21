## ADDED Requirements

### Requirement: Common LearningEvent quality rules
The system SHALL define common quality rules for required canonical fields, event time parseability, allowed action/object/category values, and event id uniqueness.

#### Scenario: Missing event time is invalid
- **WHEN** a normalized event lacks a parseable `event_time`
- **THEN** the quality validator marks the event `invalid` and records a quality error

### Requirement: Source-specific quality rules
The system SHALL allow daotao.ai to reference source-specific quality rules outside framework core.

#### Scenario: Daotao noise rule is applied
- **WHEN** a daotao.ai event matches a configured bot, crawler, or platform-noise rule
- **THEN** the quality validator marks the canonical event according to the configured daotao.ai rule

### Requirement: Invalid event output
The system SHALL retain invalid or ignored events in a reportable output such as `silver_invalid_events`, quarantine records, or quality report files for the vertical slice.

#### Scenario: Invalid event is traceable to Bronze
- **WHEN** a daotao.ai event fails quality validation
- **THEN** the invalid output includes quality errors and `raw_event_ref` so the source Bronze record can be inspected
