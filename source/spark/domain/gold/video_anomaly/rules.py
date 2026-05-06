"""Skeleton: threshold-based anomaly rules."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_rule_thresholds(df: DataFrame, min_users: int, surge_z: float) -> DataFrame:
    return df.withColumn(
        "is_anomaly",
        (F.col("distinct_users") >= F.lit(min_users)) & (F.col("surge_score") >= F.lit(surge_z)),
    )
