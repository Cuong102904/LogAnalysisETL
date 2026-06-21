## ADDED Requirements

### Requirement: Constrained MappingSpec operations
The system SHALL support MappingSpec operations `path`, `const`, `coalesce`, `cast`, `resolver`, and approved `plugin`, and MUST NOT support arbitrary Python, SQL, or free-form expressions in YAML.

#### Scenario: Mapping with supported operations succeeds
- **WHEN** daotao.ai mapping uses `path`, `const`, `cast`, and `resolver`
- **THEN** the normalizer evaluates the mapping and produces canonical `LearningEvent` fields

### Requirement: Event type resolver uses catalog
The system SHALL resolve normalized `action`, `object_type`, and `event_category` from catalog event type mappings rather than hard-coded core branches.

#### Scenario: edX problem submit event is resolved
- **WHEN** the raw event type is `edx.grades.problem.submitted`
- **THEN** action, object type, and category are resolved from `catalog/event_types/edx_tracking_log.yaml`

### Requirement: Internal plugin registry
The system SHALL allow MappingSpec to call only plugin names registered in an internal approved transform registry and MUST NOT load arbitrary Python import paths from YAML.

#### Scenario: Approved object id plugin is invoked
- **WHEN** MappingSpec references plugin `daotao_extract_object_id`
- **THEN** the normalizer invokes the registered transform if present and fails validation if the plugin name is unregistered

### Requirement: Daotao normalization output
The system SHALL normalize daotao.ai Bronze records into `silver_learning_events` using catalog mappings and registered plugins without daotao.ai-specific branches in `src/learnlake/`.

#### Scenario: Daotao fixture normalizes through catalog
- **WHEN** daotao.ai fixture Bronze records are normalized
- **THEN** Silver output conforms to `LearningEvent` and source-specific semantics come from catalog or approved plugin registry
