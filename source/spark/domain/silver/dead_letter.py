from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def to_dead_letter(df: DataFrame, reason: str) -> DataFrame:
    return df.select(F.lit(reason).alias("error_reason"), F.col("value_raw"))
