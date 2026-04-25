from pyspark.sql import DataFrame, functions as F


def optional_fanout(df: DataFrame, enabled: bool) -> DataFrame:
    if not enabled:
        return df
    return df.select(
        F.lit("mooc.unknown.events").alias("topic"),
        F.col("kafka_key").cast("binary").alias("key"),
        F.col("value_raw").cast("binary").alias("value"),
    )
