## Why

The current LearnLake pipeline is primarily streaming, with Kafka feeding Spark Bronze/Silver/Gold jobs, so lineage needs to be captured at the dataflow boundaries where events are produced, transformed, and written. Today there is no standardized OpenLineage emission path or UI to inspect the end-to-end stream lineage across Kafka topics and Delta tables.

## What Changes

- Add OpenLineage emission to the streaming dataflow built around Kafka and Spark.
- Capture lineage for Spark Bronze streaming jobs that read Kafka and write Bronze Delta.
- Capture lineage for Spark Silver streaming jobs that read Bronze Delta and write normalized Silver Delta outputs.
- Add a local lineage backend/UI, with Marquez as the preferred default for graph inspection.
- Keep the existing Spark/Kafka runtime and local stack intact; this change adds observability around the pipeline rather than changing the business flow.

## Capabilities

### New Capabilities

- `streaming-lineage`: Emit and visualize OpenLineage for the LearnLake streaming pipeline from Kafka through Spark Bronze and Silver outputs.
- `lineage-ui`: Provide a local UI/backend for browsing lineage graphs and runs, preferably via Marquez.

### Modified Capabilities

- None

## Impact

- Spark runtime configuration in `platform/local/spark/conf/spark-defaults.conf`.
- Spark image/runtime packaging in `platform/local/spark/Dockerfile` if lineage agent or jars need to be baked in.
- Streaming entrypoints in `apps/spark/run_bronze.py` and `apps/spark/run_silver.py`.
- Local stack composition in `docker-compose.yaml` to run the lineage backend/UI.
- Docs in `README.md` and `docs/local-setup.md` to explain how to enable and inspect lineage.
