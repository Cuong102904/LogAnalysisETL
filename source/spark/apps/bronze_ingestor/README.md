# Bronze Ingestor

Consumes `mooc.raw.events`, enriches ingest metadata, and writes Delta Bronze.

- Entry: `spark-submit --master spark://spark-master:7077 /opt/project/spark/apps/bronze_ingestor/main.py`
- Output path: `s3a://lakehouse/mooc/bronze/mooc_events_raw`
- Checkpoint path: `s3a://platform/mooc/bronze_ingestor`
- Spark uses its own Structured Streaming checkpoint state; do not model this as a long-lived Kafka consumer group for UI purposes.

Operational checks and maintenance are orchestrated by Airflow DAGs in `source/airflow/dags/bronze/`.
