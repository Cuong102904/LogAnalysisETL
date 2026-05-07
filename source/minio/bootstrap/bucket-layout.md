# Bucket Layout

- `bronze`: raw and lightly curated bronze Delta tables.
- `silver`: normalized Silver Delta tables.
- `gold`: aggregated Gold Delta tables.
- `checkpoints`: structured streaming checkpoints.
- `spark-events`: Spark event logs for History Server.
- `raw-archive` (optional future): immutable archive of source dumps.
