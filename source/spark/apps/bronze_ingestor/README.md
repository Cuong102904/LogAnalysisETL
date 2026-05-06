# Bronze Ingestor

Consumes `mooc.raw.events`, enriches ingest metadata, and writes Delta Bronze.

- Entry: `python -m apps.bronze_ingestor.main`
- Output path: `s3a://bronze/mooc/bronze/mooc_events_raw`
- Checkpoint path: `s3a://checkpoints/mooc/bronze_ingestor`

Operational checks and maintenance are orchestrated by Airflow DAGs in `source/airflow/dags/bronze/`.
