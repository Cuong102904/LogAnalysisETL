from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects.daotao_ai.gold.domain.common import rolling_anomaly_score, safe_ratio
from projects.daotao_ai.gold.domain.video_anomaly.time_bucketing import bucket_video_interactions


def build_video_anomaly_features(video_df: DataFrame, bucket_seconds: int = 5) -> DataFrame:
    base = bucket_video_interactions(video_df, bucket_seconds).withColumn(
        "watch_ratio", safe_ratio(F.col("current_time_s"), F.col("video_duration").cast("double"))
    )
    grouped = base.groupBy(
        "event_date",
        "course_id",
        "video_id",
        "time_bucket_s",
        "wallclock_bucket_ts",
        "action_type",
    ).agg(
        F.count("*").alias("event_count"),
        F.approx_count_distinct("user_id").alias("distinct_users"),
        F.approx_count_distinct("session_id").alias("distinct_sessions"),
        F.avg("current_time_s").alias("avg_current_time_s"),
        F.max("current_time_s").alias("max_current_time_s"),
        F.avg("video_duration").alias("avg_video_duration_s"),
        F.avg("seek_distance_s").alias("avg_seek_distance_s"),
        F.avg("watch_ratio").alias("avg_watch_ratio"),
        F.min("time").alias("first_time"),
        F.max("time").alias("last_time"),
        F.max("time").alias("last_event_time"),
    )
    scored = rolling_anomaly_score(
        grouped,
        ["course_id", "video_id", "action_type"],
        "event_count",
        ["event_date", "time_bucket_s", "wallclock_bucket_ts"],
    )
    return (
        scored.withColumn("anomaly_domain", F.lit("video"))
        .withColumn("entity_type", F.lit("video_action"))
        .withColumn("entity_id", F.concat_ws("|", F.col("video_id"), F.col("action_type")))
        .withColumn("metric_name", F.lit("event_count"))
        .withColumn("metric_value", F.col("event_count").cast("double"))
        .withColumn(
            "is_anomaly",
            F.col("is_anomaly")
            & (F.col("distinct_users") >= F.lit(2))
            & (F.col("event_count") >= F.lit(3)),
        )
    )


def aggregate_features(df: DataFrame, bucket_seconds: int = 5) -> DataFrame:
    return build_video_anomaly_features(df, bucket_seconds)
