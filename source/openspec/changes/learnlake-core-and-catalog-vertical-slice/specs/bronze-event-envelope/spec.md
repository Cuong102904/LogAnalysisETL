## ADDED Requirements

### Requirement: Common BronzeEnvelope fields
The system SHALL define a `BronzeEnvelope` contract with `event_id`, `source_id`, `source_type`, `source_event_type`, `event_time_raw`, `event_time`, `ingestion_time`, `raw_payload`, `kafka_topic`, `kafka_partition`, `kafka_offset`, `schema_version`, and `processing_date`.

#### Scenario: Daotao record is wrapped as BronzeEnvelope
- **WHEN** a daotao.ai raw record is ingested from Kafka or fixture input
- **THEN** the resulting Bronze record includes raw payload, source identity, source type, event-time metadata, ingestion metadata, schema version, and optional Kafka metadata

### Requirement: Single Bronze table for first implementation
The system SHALL write first-phase Bronze data to one `bronze_events` table partitioned by `source_id` and `processing_date`.

#### Scenario: Bronze records are partitioned by source and processing date
- **WHEN** daotao.ai records are written to Bronze
- **THEN** records are stored in `bronze_events` with `source_id = daotao_ai` and a populated `processing_date`

### Requirement: Event time comes from source payload
The system SHALL treat the configured source event-time field as the source of truth for event time and MUST NOT infer event sequence from ingest order, arrival order, or file line order.

#### Scenario: Out-of-order records preserve source event time
- **WHEN** two daotao.ai records arrive out of order
- **THEN** their Bronze `event_time` values are derived from source payload time fields rather than arrival position
