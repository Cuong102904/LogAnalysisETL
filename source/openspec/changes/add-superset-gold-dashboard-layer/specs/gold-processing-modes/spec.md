## ADDED Requirements

### Requirement: Gold outputs can be configured by processing mode
The system MUST allow each gold output to be declared as `streaming` or `batch`, and the declared mode MUST determine how the output is executed and refreshed.

#### Scenario: Streaming mode is selected
- **WHEN** a gold output is configured as `streaming`
- **THEN** the system SHALL process it continuously through micro-batch execution
- **AND** the output SHALL be refreshed without waiting for a manual batch run

#### Scenario: Batch mode is selected
- **WHEN** a gold output is configured as `batch`
- **THEN** the system SHALL process it through a scheduled batch execution
- **AND** the output SHALL be refreshed only when the batch job runs

### Requirement: Gold outputs must consume silver sources only
The system MUST build gold outputs only from silver Delta tables that belong to the declared use case, and MUST NOT read bronze, Kafka, or raw logs directly.

#### Scenario: Gold execution starts
- **WHEN** a gold transform is executed
- **THEN** it SHALL read from its declared silver input tables
- **AND** it SHALL not depend on raw ingest payloads

### Requirement: Streaming outputs preserve stable schema across micro-batches
The system MUST keep the schema of streaming gold outputs stable across successive micro-batches so downstream semantic views can query them consistently.

#### Scenario: A new micro-batch is processed
- **WHEN** new silver events arrive for a streaming gold output
- **THEN** the next micro-batch SHALL write rows using the same gold schema
- **AND** downstream consumers SHALL be able to query the updated output without schema changes

### Requirement: Batch outputs are reproducible and partition-aware
The system MUST materialize batch gold outputs as recomputable Delta tables partitioned by their serving keys where applicable.

#### Scenario: A batch job reruns the same input window
- **WHEN** the batch job is executed again for the same silver input window
- **THEN** the resulting gold table SHALL be reproducible for that window
- **AND** the table SHALL remain partitioned by its serving keys
