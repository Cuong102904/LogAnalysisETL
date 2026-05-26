from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from domain.bronze.time import parse_raw_event_time


def enrich_bronze(raw: DataFrame) -> DataFrame:
    return (
        raw.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").cast("string").alias("kafka_offset"),
            parse_raw_event_time(F.col("value").cast("string")).alias("time"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("value_raw"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
        .withColumn("ingest_hour", F.hour("ingest_ts"))
        .withColumn(
            "dedup_key",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("kafka_topic"),
                    F.col("kafka_partition").cast("string"),
                    F.col("kafka_offset"),
                ),
                256,
            ),
        )
    )
