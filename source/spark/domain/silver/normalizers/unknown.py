from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_unknown(df: DataFrame) -> DataFrame:
    """
    Catch-all for events that don't match any known silver_class.
    Stores a raw preview for debugging and schema evolution.
    """
    base = df.filter(F.col("silver_class") == "unknown")
    return base.select(
        F.col("dedup_key").alias("event_id"),
        F.col("silver_class"),
        F.to_timestamp(F.get_json_object("value_raw", "$.time")).alias("ts"),
        F.to_date(F.to_timestamp(F.get_json_object("value_raw", "$.time"))).alias(
            "event_date"
        ),
        F.get_json_object("value_raw", "$.event_source").alias("event_source"),
        F.get_json_object("value_raw", "$.event_type").alias("event_type"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        F.get_json_object("value_raw", "$.context.path").alias("path"),
        F.get_json_object("value_raw", "$.referer").alias("referer"),
        F.get_json_object("value_raw", "$.host").alias("host"),
        F.get_json_object("value_raw", "$.ip").alias("ip"),
        F.get_json_object("value_raw", "$.agent").alias("agent"),
        F.expr("substring(value_raw, 1, 2048)").alias("value_raw_preview"),
    )
