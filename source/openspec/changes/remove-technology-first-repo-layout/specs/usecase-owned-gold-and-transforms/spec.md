## ADDED Requirements

### Requirement: Use-case-owned transforms live outside framework core
The system SHALL keep source-specific transforms, extractors, and semantic enrichments outside `src/learnlake/` and under `projects/<usecase>/` or referenced catalog definitions.

#### Scenario: Use-case semantic rule changes
- **WHEN** maintainers change a source-specific interpretation or extraction rule
- **THEN** they update `projects/<usecase>/` or associated catalog files without modifying generic framework code in `src/learnlake/`

### Requirement: Use-case-owned Gold logic has an explicit home
The system SHALL allow Gold business logic that is not meaningfully generic to live under `projects/<usecase>/gold/` rather than under technology-owned runtime folders.

#### Scenario: Use-case-specific Gold metric is implemented
- **WHEN** a Gold metric depends on use-case semantics that are too specific for generic framework builders
- **THEN** its implementation lives under `projects/<usecase>/gold/` and is invoked through a thin app or workflow task boundary

### Requirement: Framework metrics remain generic
The system SHALL keep only reusable metric helpers and generic builders under `src/learnlake/metrics/`.

#### Scenario: Generic metric helper is reused
- **WHEN** multiple use cases need the same canonical aggregation behavior
- **THEN** that shared behavior is implemented in framework metrics code without embedding source-specific assumptions

### Requirement: Removed technology folders do not own Gold logic
The system SHALL NOT require Gold business logic to remain under removed or legacy technology-first folders after migration.

#### Scenario: Contributor looks for Gold business implementation
- **WHEN** a contributor needs to inspect or extend Gold logic after the migration
- **THEN** they find generic builders in `src/learnlake/metrics/` and use-case-specific builders in `projects/<usecase>/gold/`, not in legacy `spark/` paths
