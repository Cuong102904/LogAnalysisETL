from pyspark.sql import DataFrame


def technical_dedup(df: DataFrame) -> DataFrame:
    return df.withWatermark("kafka_timestamp", "1 hour").dropDuplicates(["dedup_key"])
