from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_MOOC_EVENTS_SCHEMA = StructType(
    [
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", StringType(), False),
        StructField("time", TimestampType(), True),
        StructField("kafka_key", StringType(), True),
        StructField("value_raw", StringType(), True),
        StructField("ingest_ts", TimestampType(), False),
        StructField("ingest_date", DateType(), False),
        StructField("ingest_hour", IntegerType(), False),
        StructField("dedup_key", StringType(), False),
        StructField("parse_status", StringType(), False),
        StructField("parse_error", StringType(), True),
    ]
)
