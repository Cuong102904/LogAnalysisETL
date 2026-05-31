from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def bucket_video_interactions(df: DataFrame, bucket_seconds: int = 5) -> DataFrame:
    return (
        df.withColumn("action_type", F.lower(F.col("event_type")))
        .withColumn("current_time_s", F.col("current_time").cast("double"))
        .withColumn(
            "seek_distance_s",
            F.when(
                F.col("event_type") == "seek_video",
                F.abs(F.col("new_time").cast("double") - F.col("old_time").cast("double")),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "time_bucket_s",
            (
                F.floor(F.coalesce(F.col("current_time_s"), F.lit(0.0)) / F.lit(bucket_seconds))
                * F.lit(bucket_seconds)
            ).cast("int"),
        )
        .withColumn("wallclock_bucket_ts", F.window(F.col("time"), "1 minute").start)
    )
