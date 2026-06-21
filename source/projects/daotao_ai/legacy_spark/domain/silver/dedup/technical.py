from pyspark.sql import DataFrame


def technical_dedup(df: DataFrame) -> DataFrame:
    return df.withWatermark("time", "1 hour").dropDuplicates(["dedup_key"])
