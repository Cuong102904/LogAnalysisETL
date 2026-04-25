from pyspark.sql import DataFrame, functions as F


def bucket_video_interactions(df: DataFrame, bucket_seconds: int = 5) -> DataFrame:
    return df.withColumn(
        "time_bucket_s",
        (F.floor(F.coalesce(F.col("current_time_s"), F.lit(0.0)) / F.lit(bucket_seconds)) * F.lit(bucket_seconds)).cast(
            "int"
        ),
    ).withColumn("wallclock_bucket_ts", F.window(F.col("ts"), "1 minute").start)
