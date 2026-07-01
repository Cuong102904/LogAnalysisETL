## ADDED Requirements

### Requirement: Silver runtime is stream-only
The system SHALL run Silver normalization only as a Spark Structured Streaming job and MUST NOT expose a production batch-mode Silver execution path.

#### Scenario: Silver starts in stream mode
- **WHEN** the Silver entrypoint starts for a source profile
- **THEN** it initializes a Structured Streaming pipeline over Bronze input and does not invoke a batch-only normalization branch

### Requirement: YAML is compiled once at startup
The system SHALL load Silver YAML configuration once at driver startup and compile routes, mappings, target sinks, and schema intent into a runtime plan before processing micro-batches.

#### Scenario: Runtime plan is built from YAML
- **WHEN** the Silver driver loads the source profile
- **THEN** it compiles the route set, mapping spec, and target configuration into Spark-ready expressions and sink metadata

### Requirement: Silver uses generated StructType schemas
The system SHALL use generated Spark `StructType` schemas for Silver outputs instead of instantiating per-record Pydantic validation objects on the streaming hot path.

#### Scenario: Event index schema is generated
- **WHEN** the runtime prepares the `silver_event_index` sink
- **THEN** it derives a fixed Spark schema for the table from the Silver contract fields and writes rows using that schema

### Requirement: Silver preserves a shared event index and multi-target fanout
The system SHALL write one shared `silver_event_index` row for each valid normalized Bronze event and SHALL fan out zero or more domain fact rows to configured Silver target tables from the same normalized micro-batch.

#### Scenario: Video completion produces multiple outputs
- **WHEN** a Bronze event matches a route whose targets include both `silver_video_events` and `silver_course_content_events`
- **THEN** the runtime writes one event-index row and one fact row to each configured target table

### Requirement: Silver retains unknown and invalid records
The system SHALL retain unmatched events as unknown Silver outputs and SHALL route schema-invalid or quality-invalid events to the Silver invalid output instead of silently dropping them.

#### Scenario: Unmatched event becomes unknown
- **WHEN** a valid Bronze record does not match any route in the source-pack YAML
- **THEN** the runtime writes a normalized event-index row with unknown classification and writes an unknown fact row when configured

#### Scenario: Invalid event is diverted
- **WHEN** a Bronze record fails required Silver schema or quality validation
- **THEN** the runtime writes the record to the Silver invalid output with quality errors and does not emit domain fact rows

### Requirement: Silver extractors may use UDF fallback
The system SHALL prefer native Spark DataFrame expressions for Silver extraction and routing, but MAY use narrowly scoped UDFs or pandas UDFs for extractor logic that is impractical to express natively.

#### Scenario: Hard extractor falls back to UDF
- **WHEN** a route-specific extractor cannot be expressed as a native Spark column expression
- **THEN** the compiler may bind that extractor through a limited UDF path while preserving the same target-table contract
