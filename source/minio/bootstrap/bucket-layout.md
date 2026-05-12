# Bucket Layout

- `lakehouse`: all bronze/silver/gold Delta tables, grouped by prefix.
- `platform`: structured streaming checkpoints, Spark event logs, and metastore state.
- `raw-archive` (optional future): immutable archive of source dumps.
