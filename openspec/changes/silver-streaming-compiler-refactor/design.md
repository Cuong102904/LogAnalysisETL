## Context

Silver currently normalizes Bronze records through a Python-heavy path that iterates rows on the driver and then writes per-target outputs. That model works, but it leaves the hot path outside Spark's distributed execution model and keeps a batch entrypoint around even though the repository is operating Silver as a streaming-only job.

The existing repo already has the right ingredients for a compiler-style runtime: YAML profiles for source configuration, route sets, mapping specs, and target definitions; shared contracts for event index and fact schemas; and Spark entrypoints that already run Bronze and Silver as streaming jobs.

## Goals / Non-Goals

**Goals:**
- Make Silver stream-only in production.
- Keep YAML as the source of truth for routes, mappings, target sinks, and schema intent.
- Compile YAML once on the driver into Spark-native runtime objects.
- Execute Silver normalization, validation, and fanout as distributed DataFrame transforms.
- Preserve `silver_event_index` as the common lineage table and keep multi-target fanout behavior.
- Allow narrowly scoped Python UDFs only when a rule or extractor is impractical to express with Spark SQL/DataFrame functions.

**Non-Goals:**
- Reworking Bronze ingestion.
- Reworking Gold aggregations beyond any schema or input contract fallout.
- Eliminating YAML-driven configuration.
- Forcing every existing extractor to become purely SQL-native in a single step.

## Decisions

### 1. Silver becomes stream-only
Use the streaming code path as the only supported Silver runtime. The current batch branch in `run_silver.py` is treated as dead production behavior and removed from the runtime contract.

Alternatives considered:
- Keep batch and stream side by side. Rejected because the batch path is unused in this repository and would keep two execution models in sync.
- Keep batch as a fallback. Rejected for the same reason; this change is intended to simplify, not preserve legacy behavior.

### 2. Compile YAML once at driver startup
Load source profile, route YAML, mapping YAML, and quality rules once when the Spark application starts. Translate them into a runtime plan containing route predicates, target metadata, output schemas, and field expressions.

Alternatives considered:
- Evaluate YAML per record. Rejected because it preserves the Python row-by-row bottleneck.
- Broadcast raw YAML and interpret it inside UDFs. Rejected because it still pushes core logic into per-row Python execution.

### 3. Use Spark `StructType` as the runtime schema layer
Treat the contract models as schema sources, but execute Silver writes using generated `StructType` objects. The runtime should no longer instantiate Pydantic models per record on the hot path.

Alternatives considered:
- Keep Pydantic validation in the micro-batch loop. Rejected because it is driver-centric and does not scale with distributed execution.
- Replace all contracts with ad hoc DataFrame inference. Rejected because it weakens schema stability and makes validation harder to reason about.

### 4. Normalize once, then fan out by target metadata
Build a distributed normalized DataFrame containing shared Silver metadata, route metadata, and any extracted fields needed by downstream fact tables. The runtime then fans out to configured sink targets using the route metadata from the DataFrame and the sink registry from the source profile.

Alternatives considered:
- Fan out directly from `silver_event_index` only. Rejected because fact tables need domain-specific columns that are not part of the common event-index schema.
- Keep Python extractor functions as the primary fanout mechanism. Rejected because it keeps the hot path row-oriented.

### 5. Keep UDFs as a narrow escape hatch
Some extractor behavior is regex-heavy, payload-shape-sensitive, or awkward to express in pure Spark SQL. For those cases, allow a small number of UDFs or pandas UDFs, but only after trying native DataFrame expressions first.

Alternatives considered:
- Ban UDFs entirely. Rejected because it would block a practical migration path for complex extractors.
- Make UDFs the default. Rejected because it defeats the purpose of moving to distributed Spark execution.

## Risks / Trade-offs

- [Compiler complexity] -> The YAML-to-DataFrame compiler will be more complex than the current Python mapping code. Mitigation: keep the DSL narrow, start with the current route/mapping patterns only, and add tests around compiled expressions.
- [UDF performance] -> A few extractors may still use UDFs and reduce optimizer visibility. Mitigation: keep UDF usage limited and document which extractors are not native yet.
- [Schema drift] -> Generated `StructType` definitions can diverge from contract intent if not tested. Mitigation: keep schema tests that compare generated Spark schemas against contract expectations.
- [Migration churn] -> Removing batch mode will touch entrypoints, docs, and workflow definitions. Mitigation: migrate Silver first, then clean up batch-only helpers and references in follow-up changes if needed.
- [Debuggability] -> Distributed transforms are harder to inspect than row-by-row Python. Mitigation: expose explicit route metadata, target metadata, and invalid-record reasons in the output data.

## Migration Plan

1. Introduce a compile step that turns YAML route and mapping config into Spark-native runtime objects without changing output semantics.
2. Add the stream-only distributed Silver execution path alongside the current logic until parity tests pass.
3. Switch `run_silver.py` to use the compiled streaming path as the only production entrypoint.
4. Remove the batch-mode Silver runtime branch and any batch-only helper code that is no longer needed.
5. Update docs, workflows, and tests to reflect stream-only Silver behavior.
6. If any extractor cannot be expressed natively, isolate it behind a small UDF and document it as a temporary exception.

Rollback strategy:
- Keep the old Python normalization code in version control until the new streaming path passes parity tests.
- If the compiled path fails, revert the Silver entrypoint to the previous logic while keeping the YAML/config changes intact.

## Open Questions

- Which extractors should be converted to native Spark expressions first, and which ones need temporary UDFs?
- Should the schema compiler generate `StructType` definitions directly from contract classes or from a dedicated schema registry layer?
- Should unknown/unmatched routes write only `silver_unknown_events`, or should they also keep an explicit route metadata row in the normalized stream for observability?
