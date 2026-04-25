from pyspark.sql import DataFrame, Window, functions as F


def aggregate_features(df: DataFrame) -> DataFrame:
    grouped = df.groupBy(
        "event_date", "course_id", "video_id", "time_bucket_s", "wallclock_bucket_ts", "action_type"
    ).agg(
        F.count("*").alias("event_count"),
        F.countDistinct("user_id_int").alias("distinct_users"),
        F.countDistinct("session").alias("distinct_sessions"),
    )
    window = Window.partitionBy("course_id", "video_id", "action_type").orderBy("time_bucket_s").rowsBetween(-3, 3)
    return grouped.withColumn("rolling_mean_7", F.avg("event_count").over(window)).withColumn(
        "surge_score", (F.col("event_count") - F.col("rolling_mean_7")) / F.greatest(F.lit(1.0), F.stddev("event_count").over(window))
    )
