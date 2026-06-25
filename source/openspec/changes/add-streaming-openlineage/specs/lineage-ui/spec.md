## ADDED Requirements

### Requirement: A local lineage UI is available for development
The system SHALL provide a local UI/backend that can display OpenLineage runs and dataset graphs for the streaming pipeline.

#### Scenario: Developer can inspect the lineage graph locally
- **WHEN** the OpenLineage backend receives events from the streaming jobs
- **THEN** the developer SHALL be able to inspect the resulting lineage graph in a local UI

#### Scenario: The UI supports run history
- **WHEN** multiple streaming runs are emitted
- **THEN** the UI SHALL show run history and dataset relationships for the emitted lineage events

### Requirement: Marquez is the preferred local lineage backend
The system SHALL support Marquez as the default local backend/UI for OpenLineage inspection.

#### Scenario: Local stack includes a lineage backend
- **WHEN** the local development stack is started with lineage enabled
- **THEN** it SHALL include a backend that accepts OpenLineage events and exposes the lineage graph to the developer

#### Scenario: Backend choice remains replaceable
- **WHEN** the project later adopts a different lineage viewer
- **THEN** the OpenLineage emission contract SHALL remain unchanged so the UI backend can be swapped without changing the pipeline jobs
