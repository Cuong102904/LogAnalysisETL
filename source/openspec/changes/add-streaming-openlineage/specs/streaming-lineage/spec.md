## ADDED Requirements

### Requirement: Streaming jobs emit lineage for Kafka, Bronze, and Silver
The system SHALL emit OpenLineage events for the Kafka-to-Bronze and Bronze-to-Silver parts of the streaming pipeline.

#### Scenario: Bronze streaming job is visible in lineage
- **WHEN** the Bronze streaming job reads from the Kafka raw topic and writes Bronze Delta
- **THEN** the job SHALL appear in the lineage graph with Kafka as its upstream dataset and Bronze Delta as its downstream dataset

#### Scenario: Silver streaming job is visible in lineage
- **WHEN** the Silver streaming job reads Bronze Delta and writes Silver Delta outputs
- **THEN** the job SHALL appear in the lineage graph with Bronze Delta as its upstream dataset and Silver Delta outputs as its downstream datasets

#### Scenario: Gold is excluded from the initial rollout
- **WHEN** the initial OpenLineage rollout is applied
- **THEN** Gold jobs SHALL remain out of scope until a separate follow-up change adds them

### Requirement: Streaming jobs use shared lineage defaults
The system SHALL provide shared OpenLineage defaults so Bronze and Silver jobs use a consistent namespace, transport, and listener setup.

#### Scenario: Bronze and Silver inherit the same lineage settings
- **WHEN** Bronze and Silver jobs are launched from their respective runtime entrypoints
- **THEN** both jobs SHALL use the same shared lineage defaults without duplicated per-job wiring

#### Scenario: Shared defaults reduce drift
- **WHEN** the OpenLineage transport or namespace changes
- **THEN** the change SHALL be applied through the shared configuration path used by both jobs
