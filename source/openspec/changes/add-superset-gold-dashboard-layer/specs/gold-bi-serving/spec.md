## ADDED Requirements

### Requirement: Superset must read gold through Trino semantic views
The system MUST expose gold outputs to Superset through Trino semantic views, and Superset MUST NOT query physical gold tables directly.

#### Scenario: Dashboard dataset is created
- **WHEN** a Superset dataset is registered
- **THEN** its source SHALL be a Trino view such as `delta.mooc.*_view`
- **AND** it SHALL not point to a physical Delta gold table

### Requirement: Live Ops dashboard groups live use cases in one dashboard
The system MUST provide one `Live Ops` dashboard that contains multiple sections or tabs for operational monitoring.

#### Scenario: Live Ops dashboard renders
- **WHEN** the `Live Ops` dashboard is opened
- **THEN** it SHALL include separate sections or tabs for KPI summary, time trends, alert list, and drill-down analysis
- **AND** those sections SHALL stay within the same dashboard surface

### Requirement: Live Ops dashboard refreshes automatically and manually
The system MUST configure the `Live Ops` dashboard to auto-refresh at approximately 30 seconds and MUST allow the user to trigger manual refresh on demand.

#### Scenario: Automatic refresh interval elapses
- **WHEN** 30 seconds elapse on the `Live Ops` dashboard
- **THEN** the dashboard SHALL refresh automatically

#### Scenario: Manual refresh is requested
- **WHEN** the user clicks refresh on the `Live Ops` dashboard
- **THEN** the dashboard SHALL reload immediately

### Requirement: Learning Analytics dashboard is manual refresh only
The system MUST configure the `Learning Analytics` dashboard for manual refresh only.

#### Scenario: Batch analytics are viewed
- **WHEN** the `Learning Analytics` dashboard is opened
- **THEN** it SHALL present batch-oriented sections for course trends, learner profiling, and drill-down analysis
- **AND** the user SHALL refresh it manually after batch jobs complete

### Requirement: Dashboard bootstrap is registry-driven
The system MUST define dashboard surfaces, their datasets, and their layouts in code so the BI layer can be rebuilt from repository state.

#### Scenario: Bootstrap runs
- **WHEN** the Superset bootstrap executes
- **THEN** it SHALL create the declared dashboard surfaces and their datasets from the registry definition
- **AND** the resulting dashboards SHALL match the code-defined layout and refresh policy
