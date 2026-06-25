## 1. Shared Spark OpenLineage Runtime

- [x] 1.1 Add a shared OpenLineage helper or config path for Spark jobs so Bronze and Silver can reuse the same namespace, transport, and listener settings.
- [x] 1.2 Wire the shared OpenLineage defaults into `src/learnlake/runtime/spark.py` or the chosen runtime helper without changing ETL business logic.
- [x] 1.3 Update `platform/local/spark/conf/spark-defaults.conf` with the baseline OpenLineage listener and transport defaults for local Spark runs.
- [x] 1.4 Update `platform/local/spark/Dockerfile` only if the OpenLineage agent or jars must be baked into the Spark image.

## 2. Streaming Job Instrumentation

- [x] 2.1 Apply the shared OpenLineage runtime to `apps/spark/run_bronze.py` so the Bronze streaming job emits lineage for Kafka -> Bronze Delta.
- [x] 2.2 Apply the shared OpenLineage runtime to `apps/spark/run_silver.py` so the Silver streaming job emits lineage for Bronze Delta -> Silver Delta.
- [x] 2.3 Keep Gold out of the initial rollout and confirm no Gold code changes are required for this change.
- [x] 2.4 Verify the Bronze and Silver Spark app/query names remain stable and readable in lineage output.

## 3. Local Lineage Backend and UI

- [x] 3.1 Add Marquez to `docker-compose.yaml` as the local OpenLineage backend/UI for development.
- [x] 3.2 Configure explicit memory limits for the Marquez container so lineage does not destabilize the rest of the local stack.
- [x] 3.3 Add any required compose profile or dependency wiring so Marquez can be started with the local stack when lineage is enabled.

## 4. Documentation and Validation

- [x] 4.1 Update `README.md` to document that the initial OpenLineage scope is Kafka -> Bronze -> Silver.
- [x] 4.2 Update `docs/local-setup.md` with the commands or environment switches needed to start the lineage backend and inspect the graph.
- [x] 4.3 Document the memory-footprint trade-off of the lineage backend so developers know it is an additional always-on service.
- [ ] 4.4 Validate the end-to-end graph in Marquez using a local Bronze and Silver streaming run.
