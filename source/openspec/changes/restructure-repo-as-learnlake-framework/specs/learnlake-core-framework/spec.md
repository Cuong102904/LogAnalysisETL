## ADDED Requirements

### Requirement: Source-agnostic core package
The system SHALL provide a Python package under `src/learnlake/` for reusable learning-log analytics logic, and the package MUST NOT contain source-specific identifiers such as daotao.ai names, edX event names, dataset file names, fixed Kafka topics, or fixed Delta paths.

#### Scenario: Core package contains no daotao-specific logic
- **WHEN** maintainers inspect or test files under `src/learnlake/`
- **THEN** the package contains reusable contracts, engines, connectors, runtime helpers, and metrics without hard-coded daotao.ai or edX/MOOC rules

### Requirement: Stable public framework APIs
The system SHALL expose stable public APIs for loading source profiles, creating Spark runtime sessions, running Bronze ingestion, running Silver normalization, validating data quality, and building Gold metrics.

#### Scenario: Thin Spark entrypoint calls framework API
- **WHEN** a Spark entrypoint runs `run_silver.py --source daotao_ai`
- **THEN** the entrypoint loads the daotao.ai source profile and calls `learnlake` normalization APIs instead of implementing source-specific normalization itself

### Requirement: Common Bronze event envelope
The system SHALL define a common Bronze event envelope that preserves source identity, source event type where available, event-time raw value, ingestion time, raw payload, schema version, and runtime metadata such as Kafka topic, partition, and offset when available.

#### Scenario: Kafka raw event becomes Bronze envelope
- **WHEN** a raw daotao.ai Kafka record is ingested
- **THEN** the Bronze output preserves the raw payload and records source identity, event-time metadata, ingestion metadata, schema version, and Kafka metadata

### Requirement: Canonical LearningEvent contract
The system SHALL define `LearningEvent` as the canonical Silver contract for normalized learning activity across source profiles.

#### Scenario: Daotao event normalizes to LearningEvent
- **WHEN** a daotao.ai raw event is normalized through its source profile and mapping
- **THEN** the output conforms to the canonical `LearningEvent` required fields and can be written to `silver_learning_events`

### Requirement: Config-driven normalization engine
The system SHALL provide a normalization engine that maps Bronze records to `LearningEvent` using catalog mapping declarations, event type resolvers, expression helpers, timestamp handling, deduplication, and optional plugin hooks.

#### Scenario: Event type is resolved from catalog
- **WHEN** the normalizer sees an edX event type such as `play_video`
- **THEN** it resolves action and object type from the configured catalog event type map rather than from hard-coded core Python branches

### Requirement: Quality validation engine
The system SHALL provide a quality validation engine that evaluates common and source-specific rules against Bronze, Silver, or metric outputs and can mark, report, or quarantine invalid records.

#### Scenario: Required LearningEvent fields are validated
- **WHEN** a normalized event is missing a required `LearningEvent` field such as `actor_id` or `event_time`
- **THEN** the validator applies configured quality rules and reports or routes the invalid record according to the source profile

### Requirement: Generic metric builders
The system SHALL provide generic Gold metric builders that consume canonical Silver data or standardized Silver projections instead of reading raw source payloads directly.

#### Scenario: Learner activity metric reads canonical Silver
- **WHEN** the learner activity metric is built for daotao.ai
- **THEN** the metric builder reads `silver_learning_events` or approved Silver projections and does not parse daotao.ai raw payloads directly
