from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects.daotao_ai.gold.domain.common import ensure_event_date


def bucket_video_interactions(df: DataFrame, bucket_seconds: int = 5) -> DataFrame:
    bucketed = (
        df.withColumn("user_id", F.col("actor_id").cast("long"))
        .withColumn("time", F.col("event_time"))
        .withColumn("action_type", F.lower(F.col("video_action")))
        .withColumn("current_time_s", F.col("current_time_seconds").cast("double"))
        .withColumn("video_duration", F.col("duration_seconds").cast("double"))
        .withColumn(
            "seek_distance_s",
            F.when(
                F.col("action_type") == "seek",
                F.abs(
                    F.col("new_time_seconds").cast("double")
                    - F.col("old_time_seconds").cast("double")
                ),
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
    return ensure_event_date(bucketed)
