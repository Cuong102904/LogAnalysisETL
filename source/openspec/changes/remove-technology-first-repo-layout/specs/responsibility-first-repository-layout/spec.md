## ADDED Requirements

### Requirement: Responsibility-first repository root
The system SHALL organize the repository root by responsibility boundaries rather than by technology-owned top-level folders.

#### Scenario: Contributor inspects the repository root
- **WHEN** a contributor reads the repository top-level layout
- **THEN** reusable framework code is under `src/learnlake/`, declarative configuration is under `catalog/`, thin entrypoints are under `apps/`, use-case code is under `projects/`, infrastructure assets are under `platform/local/`, workflow runners are under `orchestration/`, serving assets are under `serving/`, and tests are under `tests/`

### Requirement: Legacy technology folders are removed after migration
The system SHALL remove top-level technology-owned folders such as `spark/`, `kafka/`, `airflow/`, `trino/`, and `superset/` after their responsibilities have been migrated and the new paths are active.

#### Scenario: Migration is complete
- **WHEN** runtime entrypoints, Docker/runtime mounts, orchestration code, serving assets, and documentation no longer reference legacy top-level technology owners
- **THEN** the repository no longer contains supported top-level folders named `spark/`, `kafka/`, `airflow/`, `trino/`, or `superset/`

### Requirement: Technology assets move to explicit owners
The system SHALL place technology-specific assets under explicit responsibility folders instead of preserving them as root owners.

#### Scenario: Service configuration is located
- **WHEN** a maintainer needs Kafka, Spark, MinIO, Hive, Trino, or Superset runtime configuration
- **THEN** those assets are located under `platform/local/`, `orchestration/`, or `serving/` according to their responsibility

### Requirement: Thin runtime entrypoints remain supported
The system SHALL keep runnable entrypoints under `apps/` and MUST NOT require contributors to run jobs from removed legacy top-level technology folders.

#### Scenario: Bronze job is executed
- **WHEN** a Bronze or Silver job is started for a use case
- **THEN** the documented and supported command path uses `apps/` entrypoints rather than legacy `spark/apps/*` or other removed technology-first paths
