## ADDED Requirements

### Requirement: Multi-target normalization result
The normalization engine SHALL return a multi-target result for each Bronze record containing at most one event-index record, zero or more domain fact records, and zero or more invalid records.

#### Scenario: Video event produces index and fact
- **WHEN** a daotao.ai browser `play_video` Bronze record is normalized
- **THEN** the result contains one `silver_event_index` record and one `silver_video_events` fact record

#### Scenario: Noise event produces index and system fact
- **WHEN** a daotao.ai bot or scanner event is normalized
- **THEN** the result contains an event-index record marked as noise and either a system/noise fact record or an unknown/system route according to the source-pack routing configuration

#### Scenario: Invalid event produces invalid output
- **WHEN** a Bronze record cannot produce required event-index fields
- **THEN** the result contains an invalid record and no domain fact rows

### Requirement: Source profile declares Silver targets
The source profile SHALL support configuring a Silver event-index target, multiple Silver domain targets, invalid output, table names, paths, schemas, checkpoints, quality rules, and routing/mapping references.

#### Scenario: Runtime loads daotao Silver targets
- **WHEN** the daotao.ai source profile is loaded
- **THEN** the profile exposes configured targets for event index and relevant domain fact tables instead of a single `silver_learning_events` target only

#### Scenario: Missing target path fails validation
- **WHEN** a source profile enables a Silver target without a write path
- **THEN** profile validation fails before the Silver job starts

### Requirement: Silver runtime writes per target
The Silver runtime SHALL group normalized output records by target table and write each group to the configured sink path using the configured schema and checkpoint.

#### Scenario: Batch writes multiple outputs
- **WHEN** one micro-batch contains video, assessment, and PDF events
- **THEN** the runtime writes records to `silver_event_index`, `silver_video_events`, `silver_assessment_events`, and `silver_document_events` as applicable

#### Scenario: Empty target is skipped
- **WHEN** a micro-batch has no records for a configured target
- **THEN** the runtime skips writing that target for the batch without failing the job

### Requirement: Per-target schemas and quality validation
The system SHALL validate event-index and domain fact records against their target schemas and quality rules before writing valid records to Silver.

#### Scenario: Domain fact type validation catches bad field
- **WHEN** a video extractor emits a non-numeric value for a numeric playback field
- **THEN** validation records a quality error and routes the affected output according to invalid-output policy

#### Scenario: Event index and fact quality are independent
- **WHEN** an event-index row is valid but one optional domain fact extraction fails
- **THEN** the system preserves the valid event-index row and reports the fact extraction failure according to configured quality policy

### Requirement: Unknown and unmatched events are retained
The system SHALL retain events that do not match source-pack routes as unknown events rather than silently dropping them.

#### Scenario: Unknown daotao event is retained
- **WHEN** a valid daotao.ai event has no matching routing rule
- **THEN** the system writes an event-index row with unknown classification and writes an unknown fact or invalid/unknown output according to the configured unknown policy

### Requirement: Streaming and batch parity
The normalization behavior SHALL be consistent between batch fixture mode and Spark streaming mode for the same Bronze input records.

#### Scenario: Fixture and stream produce equivalent normalized records
- **WHEN** the same daotao.ai fixture events are processed in batch mode and in streaming micro-batches
- **THEN** the normalized event-index and domain fact records contain equivalent event ids, event times, normalized types, and extracted typed fields

### Requirement: Local Docker Compose verification
The implementation SHALL include a final verification step that can start the requested local runtime subset with Docker Compose and verify that Bronze and Silver streams run without source-specific framework-core branches.

#### Scenario: Requested local services start
- **WHEN** the verifier runs `docker compose up -d broker1 broker2 broker3 kafka-ui kafka-init minio minio-init bronze-stream silver-stream tracking-log-replayer spark-master spark-worker-1`
- **THEN** the requested services start or report actionable configuration errors

#### Scenario: Silver stream writes expected targets
- **WHEN** the tracking-log replayer publishes daotao.ai events and Bronze stream ingests them
- **THEN** the Silver stream writes event-index records and at least one relevant domain fact target for the replayed sample

#### Scenario: Verification documents failures
- **WHEN** Docker Compose verification cannot complete because of environment, image, or service readiness issues
- **THEN** the verification result records the failing service, command output summary, and the next action needed to reproduce or fix the issue
