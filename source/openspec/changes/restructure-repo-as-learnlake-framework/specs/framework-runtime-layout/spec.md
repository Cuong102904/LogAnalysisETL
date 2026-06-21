## ADDED Requirements

### Requirement: Framework-first repository layout
The system SHALL organize the repository around framework core, source catalog, executable apps, project case studies, local platform runtime, orchestration, serving, and tests.

#### Scenario: Top-level folders communicate ownership
- **WHEN** a contributor inspects the repository root
- **THEN** reusable logic is under `src/learnlake/`, declarations are under `catalog/`, runnable entrypoints are under `apps/`, case studies are under `projects/`, local runtime is under `platform/local/`, scheduling is under `orchestration/`, serving assets are under `serving/`, and tests are under `tests/`

### Requirement: Thin executable apps
The system SHALL keep executable app modules thin so they parse arguments, load profiles, initialize runtime objects, and call framework APIs without owning reusable business logic.

#### Scenario: Silver app delegates to framework
- **WHEN** `apps/spark/run_silver.py --source daotao_ai` is executed
- **THEN** the app delegates normalization to `learnlake` using the daotao.ai source profile

### Requirement: Replay app separated from ingestion core
The system SHALL place static dataset replay into `apps/replay/` and keep it separate from Bronze ingestion core.

#### Scenario: Static daotao logs are replayed to Kafka
- **WHEN** `apps/replay/replay_to_kafka.py --source daotao_ai --speed 10` is executed
- **THEN** the replay app reads static source data, respects source event time for pacing, and publishes raw events to the configured Kafka topic without writing Bronze tables

### Requirement: Daotao case study project folder
The system SHALL represent daotao.ai as a case study under `projects/daotao_ai/` with documentation, data dictionary, mapping notes, sample data, and optional project-specific transforms.

#### Scenario: Contributor reviews daotao assumptions
- **WHEN** a contributor needs to understand daotao.ai raw fields and mapping decisions
- **THEN** they can read `projects/daotao_ai/` documentation without inspecting framework core code

### Requirement: Runtime platform folder
The system SHALL place local Docker/runtime platform assets under `platform/local/` rather than treating Kafka, Spark, MinIO, Hive metastore, Trino, or Superset as framework logic.

#### Scenario: Local stack is started
- **WHEN** the local deployment is started through Docker Compose
- **THEN** platform service configuration is resolved from `platform/local/` and framework logic remains in `src/learnlake/`

### Requirement: Airflow orchestration folder
The system SHALL place Airflow DAGs and tasks under `orchestration/airflow/` and keep Airflow responsible for scheduling framework apps rather than implementing transformations.

#### Scenario: Airflow schedules daotao pipeline
- **WHEN** an Airflow DAG runs the daotao.ai Bronze-to-Gold workflow
- **THEN** DAG tasks invoke thin apps or Spark submit commands and do not contain normalization or metric business logic

### Requirement: Serving assets folder
The system SHALL place Trino views, Superset bootstrap logic, and serving-specific assets under `serving/`.

#### Scenario: BI serving views are managed
- **WHEN** Trino views or Superset dashboards are bootstrapped
- **THEN** serving configuration is loaded from `serving/` and reads Gold or canonical Silver outputs

### Requirement: Framework contract test organization
The system SHALL include contract tests that validate source profiles, mappings, canonical `LearningEvent` outputs, and the invariant that source-specific additions do not modify framework core.

#### Scenario: Daotao fixture proves framework behavior
- **WHEN** contract tests run against daotao.ai raw fixtures
- **THEN** the tests verify that mappings produce valid `LearningEvent` records through `learnlake` APIs
