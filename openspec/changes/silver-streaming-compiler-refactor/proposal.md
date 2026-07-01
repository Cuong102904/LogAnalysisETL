## Why

The current Silver runtime still normalizes Bronze records by pulling each micro-batch to the driver and iterating row-by-row in Python. That caps throughput, keeps the hot path off Spark's distributed execution model, and preserves an unused batch path even though this repository is operating as streaming-only for Silver.

This change makes Silver a stream-only, compiler-driven pipeline: YAML remains the source of truth for routes, mappings, targets, and schemas, but Spark compiles that configuration into DataFrame expressions and executes normalization, validation, and fanout in a distributed way.

## What Changes

- **BREAKING** Remove Silver batch-mode runtime behavior from the production entrypoint; Silver becomes stream-only.
- Compile Silver YAML configuration at driver startup into a runtime plan instead of evaluating records one by one in Python.
- Preserve YAML for routing, field mapping, target sinks, and schema intent, but execute those rules through Spark `Column` expressions and `StructType` schemas.
- Move Silver normalization to a distributed DataFrame flow that produces a shared `silver_event_index` plus fanout fact tables from the same normalized micro-batch.
- Keep multi-target routing semantics, including unknown and invalid outputs, but represent target membership as DataFrame metadata rather than driver-side record loops.
- Allow Python UDFs only as a fallback for extractor logic that is not practical to express natively in Spark expressions.
- Update Silver tests, docs, and workflow definitions to match the stream-only runtime contract.

## Capabilities

### New Capabilities
- `silver-streaming-normalization`: Stream-only Silver normalization that compiles YAML routes, mappings, and schemas into distributed Spark DataFrame transforms, writes `silver_event_index` and fanout fact tables, and routes invalid or unknown records without driver-side row iteration.

### Modified Capabilities
- None.

## Impact

- `apps/spark/run_silver.py` changes from a mixed batch/stream entrypoint to a stream-only orchestrator.
- `learnlake.normalization` becomes compiler/runtime oriented instead of row-by-row Python normalization.
- `learnlake.contracts` remains the canonical source for field intent, but Silver runtime will consume generated `StructType` schemas rather than Pydantic validation per record.
- `projects/daotao_ai/transforms.py` will be refactored toward Spark-native expressions or narrowly scoped UDFs.
- Silver workflow, tests, and docs must stop describing batch Silver behavior and reflect distributed streaming fanout only.
