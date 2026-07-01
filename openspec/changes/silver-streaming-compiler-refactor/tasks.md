## 1. Stream-Only Silver Runtime

- [ ] 1.1 Remove the Silver batch-mode execution branch from `apps/spark/run_silver.py` and keep only the streaming entrypoint path.
- [ ] 1.2 Replace the driver-side `toLocalIterator()` normalization loop with a micro-batch DataFrame pipeline skeleton.
- [ ] 1.3 Keep `foreachBatch` only as a sink orchestrator for distributed DataFrame outputs.

## 2. Compile YAML Into Runtime Plan

- [ ] 2.1 Add a Silver runtime compiler that loads source profile, routing YAML, mapping YAML, and quality rules once at driver startup.
- [ ] 2.2 Compile route definitions into Spark predicate expressions plus route metadata columns such as `route_id` and `route_targets`.
- [ ] 2.3 Compile mapping definitions into Spark column expressions for the shared Silver event-index output.

## 3. Schema and Validation Refactor

- [ ] 3.1 Generate Silver output `StructType` schemas from the contract layer instead of instantiating Pydantic models per record in the hot path.
- [ ] 3.2 Add distributed validation and invalid-record routing for schema violations and quality-rule failures.
- [ ] 3.3 Preserve unknown-route handling so unmatched events still produce normalized lineage plus unknown output when configured.

## 4. Distributed Fanout

- [ ] 4.1 Build the shared `silver_event_index` DataFrame from compiled mapping and route metadata.
- [ ] 4.2 Fan out domain fact outputs from the normalized DataFrame to configured target sinks using target metadata from YAML.
- [ ] 4.3 Allow narrowly scoped UDF or pandas UDF fallback only for extractors that cannot be expressed natively.

## 5. Tests, Docs, and Workflow Cleanup

- [ ] 5.1 Add parity tests that prove the compiled streaming Silver path produces the expected event-index and fact-table outputs for representative fixtures.
- [ ] 5.2 Update Silver workflow and runtime docs to describe the stream-only contract and remove batch-mode references.
- [ ] 5.3 Remove or deprecate batch-only helper references from tests and docs once the stream-only path is in place.
