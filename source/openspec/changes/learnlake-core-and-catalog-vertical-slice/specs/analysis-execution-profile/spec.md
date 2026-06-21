## ADDED Requirements

### Requirement: Metric execution metadata
The system SHALL allow metric definitions to declare execution metadata including `mode`, `priority`, `trigger_interval`, `enabled`, and resource hints.

#### Scenario: Metric execution profile is loaded
- **WHEN** the Gold app loads `gold_course_activity_summary`
- **THEN** it reads execution metadata from the metric definition and exposes it to runtime orchestration code

### Requirement: Execution profile is declarative only
The system SHALL NOT implement a custom scheduler or resource optimizer in this change; Airflow and Spark remain responsible for actual job execution.

#### Scenario: Priority does not schedule jobs by itself
- **WHEN** a metric definition declares `priority: high`
- **THEN** the framework records the priority as metadata but does not perform independent scheduling
