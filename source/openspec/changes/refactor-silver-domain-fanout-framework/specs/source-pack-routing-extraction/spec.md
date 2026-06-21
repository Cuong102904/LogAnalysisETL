## ADDED Requirements

### Requirement: Source pack owns source-specific semantics
The system SHALL keep source-specific event semantics, routing rules, extractor functions, quality rules, fixtures, and mapping notes outside `src/learnlake/`, with daotao.ai implemented as a source pack under `projects/daotao_ai` and/or catalog files referenced by its source profile.

#### Scenario: Core contains no daotao or Open edX routing branches
- **WHEN** tests inspect files under `src/learnlake/`
- **THEN** framework core contains no hard-coded daotao.ai identifiers, Open edX event names, fixed daotao topics, or fixed daotao Delta paths

#### Scenario: Daotao semantics live in source pack
- **WHEN** maintainers need to change how `problem_check`, `problem_graded`, `play_video`, or `textbook.pdf.*` events are interpreted
- **THEN** they update daotao.ai source-pack routing, mapping, or extractor files rather than modifying framework core

### Requirement: Declarative routing rules
The system SHALL support declarative source-pack routing rules that match raw event fields and assign `event_group`, `normalized_type`, `action`, `object_type`, target Silver tables, route priority, and an approved extractor name.

#### Scenario: Assessment route selects target tables
- **WHEN** a daotao.ai route matches browser `problem_check`
- **THEN** the route assigns the assessment group, a normalized submit type, `silver_event_index`, `silver_assessment_events`, and the configured daotao assessment extractor

#### Scenario: Route priority resolves overlap
- **WHEN** a daotao.ai `/courses/.../xblock/.../problem_check` event also matches generic course route patterns
- **THEN** the higher-priority assessment route wins and the event is routed as assessment rather than generic course content

### Requirement: Constrained route matching operators
The routing engine SHALL support constrained matching operators such as equality, membership, prefix, substring, regular expression, all/any/not composition, event source checks, host checks, context key checks, and parsed payload key checks, and MUST NOT evaluate arbitrary Python, SQL, or shell expressions from route files.

#### Scenario: Regex route matches video state save
- **WHEN** a route uses a regular expression to match Open edX video `save_user_state` paths
- **THEN** the router evaluates the regex against configured raw fields without executing arbitrary code

#### Scenario: Unsupported route operation fails validation
- **WHEN** a source-pack route declares an unsupported operator
- **THEN** source-profile validation fails before normalization starts

### Requirement: Approved extractor registry
The system SHALL invoke only extractor names registered in an approved registry and MUST NOT load arbitrary Python import paths from YAML route or mapping files.

#### Scenario: Registered daotao extractor runs
- **WHEN** a daotao.ai route references a registered extractor such as `daotao_video_browser`
- **THEN** the normalization engine invokes that extractor with the raw record, parsed payload, event-index draft, and route metadata

#### Scenario: Unregistered extractor is rejected
- **WHEN** a route references an extractor name that is not registered
- **THEN** source-profile validation or normalization fails with a quality/configuration error

### Requirement: Extractors do not classify events
Extractor functions SHALL extract typed fields for an already selected target and MUST NOT decide the event group or target table.

#### Scenario: Extractor receives resolved route
- **WHEN** the video extractor runs for a daotao.ai `pause_video` event
- **THEN** it receives the resolved route and emits video fields without reclassifying the event as video

### Requirement: Source pack registration
The system SHALL allow a source profile to register source-pack plugins and extractors during runtime initialization before normalization begins.

#### Scenario: Daotao source registers plugins
- **WHEN** `run_silver.py --source daotao_ai` initializes
- **THEN** daotao.ai transform and extractor functions are registered before routing and extraction are evaluated

### Requirement: New source without framework core changes
The system SHALL allow adding a new learning-log source by adding source profile, routing rules, mappings, fixtures, quality rules, and optional source-pack extractors without editing `src/learnlake/`.

#### Scenario: Future EdNet source is added through source pack
- **WHEN** an EdNet source profile is added later
- **THEN** implementers can add EdNet catalog and project files without changing framework core routing or normalization behavior
