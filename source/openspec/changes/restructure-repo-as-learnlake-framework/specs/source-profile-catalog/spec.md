## ADDED Requirements

### Requirement: Source profile declaration
The system SHALL define each data source through a source profile under `catalog/sources/<source_id>.yaml`.

#### Scenario: Daotao source profile is loaded
- **WHEN** a runtime command is invoked with `--source daotao_ai`
- **THEN** the system loads `catalog/sources/daotao_ai.yaml` and uses it as the source of truth for input, Bronze, Silver, quality, metric, and checkpoint configuration

### Requirement: Source type separated from source identity
The system SHALL distinguish source identity from source format or type so that `daotao_ai` can be represented as a concrete source using an `edx_tracking_log` format.

#### Scenario: Daotao is modeled as edX-based source
- **WHEN** the daotao.ai source profile is read
- **THEN** it identifies the source as `daotao_ai` and the source type or format as `edx_tracking_log`

### Requirement: Declarative field mapping
The system SHALL define source-to-`LearningEvent` mappings in catalog mapping files that support field paths, constants, expressions, resolvers, type conversions, and raw event references.

#### Scenario: Actor id is mapped from raw payload
- **WHEN** daotao.ai mapping is evaluated
- **THEN** `actor_id` is populated from the configured raw payload path rather than from hard-coded source-specific code in `src/learnlake/`

### Requirement: Event type mapping outside core
The system SHALL define source-format event type mappings under `catalog/event_types/` instead of hard-coding event names in the framework package.

#### Scenario: edX event maps to normalized action
- **WHEN** `edx.grades.problem.submitted` is processed for daotao.ai
- **THEN** its normalized action, object type, and category are resolved from `catalog/event_types/edx_tracking_log.yaml`

### Requirement: Quality rule catalog
The system SHALL define common and source-specific quality rules under `catalog/quality/` and allow source profiles to reference one or more rule files.

#### Scenario: Common and daotao rules are combined
- **WHEN** daotao.ai Silver output is validated
- **THEN** the validator applies common `LearningEvent` rules and daotao.ai-specific rules declared by the source profile

### Requirement: Metric definition catalog
The system SHALL define metric selections and metric configuration under `catalog/metrics/` so metric builders can be reused across source profiles.

#### Scenario: Course engagement metric is configured
- **WHEN** a Gold app builds `course_engagement` for daotao.ai
- **THEN** the app loads metric configuration from `catalog/metrics/` and applies it to canonical Silver data

### Requirement: Optional source-specific plugin boundary
The system SHALL allow source profiles or mappings to reference optional source-specific plugin functions for transformations that cannot be expressed declaratively, and those plugins MUST live outside `src/learnlake/`.

#### Scenario: Source-specific object id extraction uses plugin
- **WHEN** daotao.ai requires object id extraction from an edX URL pattern that is not supported by generic path mapping
- **THEN** the mapping can reference a daotao.ai plugin outside `src/learnlake/` while the core mapper only invokes the configured plugin boundary

### Requirement: New source without core modification
The system SHALL support adding a new source by adding catalog files, project documentation, fixtures, and optional plugins without modifying files under `src/learnlake/`.

#### Scenario: EdNet profile is added later
- **WHEN** EdNet support is added after daotao.ai
- **THEN** implementers add EdNet source profile, mappings, rules, samples, and tests without changing framework core behavior
