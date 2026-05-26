# DATN Source Codex Guidance

This workspace is organized by domain repository folders under `source`.

- `airflow`: orchestration, DAG wiring, task wrappers, plugins, and scheduling utilities.
- `kafka`: event ingestion, topic contracts, schemas, router/validator, and integration tests.
- `spark`: ETL jobs, transformations, data access layer, models, and Spark tests.
- `infra-central`: infrastructure composition, container topology, and deployment scaffolding.
- `docs`: architecture notes, runbooks, and operational documentation.

When implementing changes:

- Keep business logic in the owning domain folder.
- Avoid copying shared logic across `kafka`, `spark`, and `airflow`; extract to an appropriate shared module when needed.
- Keep runtime contracts aligned with documented Kafka topic names and service DNS in repo docs.
- If a change affects multiple domains, update both code and corresponding docs in `docs`.

Centralized ingest can reorder or delay lines.

- Use the input `time` field as the source of truth for when the event happened.
- Do not trust ingest order, arrival order, or line order as proof that one event happened before another.
- Do not infer causality or strict sequencing from event position alone.
- Only treat event A as preceding event B when that ordering is explicitly supported by the `time` field and domain logic.

Python execution policy:

- Activate the virtual environment before running Python commands.
- Prefer `uv run` for Python commands inside this repository.
- Keep dependencies centralized in `source/pyproject.toml`.
