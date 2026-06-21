## ADDED Requirements

### Requirement: SourceProfile declaration
The system SHALL define daotao.ai through `catalog/sources/daotao_ai.yaml` containing source identity, source type, input settings, event-time field, Bronze settings, Silver mapping reference, quality rule references, and metric references.

#### Scenario: Runtime loads daotao SourceProfile
- **WHEN** a vertical-slice app is invoked with `--source daotao_ai`
- **THEN** the system loads `catalog/sources/daotao_ai.yaml` as the source of truth for that run

### Requirement: Source identity separated from source type
The system SHALL represent `daotao_ai` as the concrete source id and `edx_tracking_log` as the source type or format.

#### Scenario: Daotao profile identifies source and format
- **WHEN** the daotao.ai source profile is validated
- **THEN** it contains `source_id = daotao_ai` and `source_type = edx_tracking_log`

### Requirement: Source-specific declarations outside core
The system SHALL store daotao.ai mappings, edX event type maps, daotao quality rules, fixtures, and project notes outside `src/learnlake/`.

#### Scenario: Event type mapping changes without core edit
- **WHEN** a daotao.ai event type mapping is updated
- **THEN** the change is made in `catalog/event_types/edx_tracking_log.yaml` without modifying `src/learnlake/`
