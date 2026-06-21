## ADDED Requirements

### Requirement: Clear Spark package boundaries
The Spark repository MUST separate runtime entrypoints, orchestration, business logic, schemas, infrastructure adapters, shared helpers, and configuration files into distinct top-level areas.

#### Scenario: A contributor looks for where to add new code
- **WHEN** a contributor adds a new Spark feature or job
- **THEN** the placement of that code SHALL be determined by responsibility, not by convenience

### Requirement: Entrypoints remain stable
The Spark repository MUST keep runnable job entrypoints available under `spark/apps/*/main.py` so existing runtime submit targets remain valid.

#### Scenario: Airflow submits a Spark job
- **WHEN** Airflow or docker compose submits a Spark job
- **THEN** the submit target SHALL continue to point at a `main.py` entrypoint under `spark/apps/`

### Requirement: Orchestration is isolated
The Spark repository MUST place read-transform-write coordination logic in pipeline modules rather than in entrypoint modules or domain modules.

#### Scenario: A job needs multiple read and write steps
- **WHEN** a Spark job coordinates several inputs, transforms, and outputs
- **THEN** the coordination logic SHALL live in `pipelines/`
- **AND THEN** `apps/*/main.py` SHALL remain a thin launcher

### Requirement: Schema definitions have one canonical home
The Spark repository MUST keep data shape definitions in a dedicated `schemas/` tree rather than scattering them across business logic modules or legacy schema folders.

#### Scenario: A table schema changes
- **WHEN** a Bronze, Silver, or Gold table schema changes
- **THEN** the schema definition SHALL be updated in `schemas/`
- **AND THEN** downstream code SHALL import that shape from the canonical schema location
- **AND THEN** the legacy `domain/schemas/` tree SHALL not be used as a source of truth

### Requirement: Shared helpers remain generic
The Spark repository MUST keep generic technical helpers separate from domain-specific logic so helpers do not become a catch-all module.

#### Scenario: A helper is reused across multiple domains
- **WHEN** a helper is used by multiple unrelated Spark features
- **THEN** the helper SHALL live in `shared/` or `infrastructure/`
- **AND THEN** domain-specific behavior SHALL remain in `domain/`

### Requirement: Legacy wrapper folders are removed
The Spark repository MUST remove old compatibility wrappers after the canonical layout is established so the filesystem and import graph reflect only the new architecture.

#### Scenario: A contributor inspects the Spark package tree
- **WHEN** the refactor is complete
- **THEN** the repository SHALL not depend on `domain/schemas/`, `utils/`, or `apps/*/job.py`
- **AND THEN** imports SHALL resolve through the canonical folders only

### Requirement: Spark runtime image includes refactored package roots
The Spark container image MUST package the refactored top-level Spark package roots so runtime jobs can import `apps/`, `pipelines/`, `schemas/`, `shared/`, `domain/`, `infrastructure/`, and `configs/` inside Docker or Compose.

#### Scenario: A Spark job starts in Docker Compose
- **WHEN** a stream launches inside the Spark image
- **THEN** Python imports SHALL resolve the refactored package layout without extra path hacks
