from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def _maybe_column(df: DataFrame, name: str, dtype: str = "string") -> Column:
    if name in df.columns:
        return F.col(name)
    return F.lit(None).cast(dtype)


def _require_columns(df: DataFrame, *columns: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def _bucket_from_time(column_name: str, size_seconds: int) -> Column:
    return F.floor(F.col(column_name) / F.lit(size_seconds)).cast("long") * F.lit(size_seconds)


def build_video_events_base(df: DataFrame) -> DataFrame:
    _require_columns(df, "event_time_utc", "course_id", "user_id", "video_id", "action_type")
    base = (
        df.select(
            F.col("event_time_utc").alias("event_time_utc"),
            F.col("username").alias("username") if "username" in df.columns else F.lit(None).cast("string").alias("username"),
            F.col("user_id").cast("string").alias("user_id"),
            _maybe_column(df, "session_id").alias("session_id"),
            F.col("course_id").cast("string").alias("course_id"),
            F.col("video_id").cast("string").alias("video_id"),
            F.coalesce(_maybe_column(df, "video_block_id").cast("string"), F.col("video_id").cast("string")).alias(
                "video_block_id"
            ),
            F.coalesce(_maybe_column(df, "video_code").cast("string"), F.col("video_id").cast("string")).alias(
                "video_code"
            ),
            F.col("action_type").alias("action_type"),
            _maybe_column(df, "duration_s", "double").alias("duration_s"),
            _maybe_column(df, "current_time_s", "double").alias("current_time_s"),
            _maybe_column(df, "old_time_s", "double").alias("old_time_s"),
            _maybe_column(df, "new_time_s", "double").alias("new_time_s"),
            _maybe_column(df, "old_speed", "double").alias("old_speed"),
            _maybe_column(df, "new_speed", "double").alias("new_speed"),
            _maybe_column(df, "speed", "double").alias("speed"),
            _maybe_column(df, "seek_type").alias("seek_type"),
            _maybe_column(df, "watch_ratio", "double").alias("watch_ratio"),
            _maybe_column(df, "position_bucket_10s", "long").alias("position_bucket_10s"),
            _maybe_column(df, "position_bucket_30s", "long").alias("position_bucket_30s"),
        )
        .filter(F.col("event_time_utc").isNotNull())
        .filter(F.col("course_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("video_id").isNotNull())
        .filter(F.col("action_type").isNotNull())
        .withColumn("event_date", F.to_date("event_time_utc"))
        .withColumn(
            "position_bucket_10s",
            F.coalesce(F.col("position_bucket_10s"), _bucket_from_time("current_time_s", 10)),
        )
        .withColumn(
            "position_bucket_30s",
            F.coalesce(F.col("position_bucket_30s"), _bucket_from_time("current_time_s", 30)),
        )
        .withColumn(
            "seek_direction",
            F.when(
                (F.col("action_type") == F.lit("seek_video")) & F.col("new_time_s").isNotNull() & F.col("old_time_s").isNotNull() & (F.col("new_time_s") > F.col("old_time_s")),
                F.lit("forward"),
            )
            .when(
                (F.col("action_type") == F.lit("seek_video")) & F.col("new_time_s").isNotNull() & F.col("old_time_s").isNotNull() & (F.col("new_time_s") < F.col("old_time_s")),
                F.lit("backward"),
            )
            .when(F.col("action_type") == F.lit("seek_video"), F.lit("same"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "seek_distance_s",
            F.when(
                (F.col("action_type") == F.lit("seek_video")) & F.col("new_time_s").isNotNull() & F.col("old_time_s").isNotNull(),
                F.abs(F.col("new_time_s") - F.col("old_time_s")),
            ),
        )
        .withColumn("is_load", F.when(F.col("action_type") == F.lit("load_video"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_play", F.when(F.col("action_type") == F.lit("play_video"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_pause", F.when(F.col("action_type") == F.lit("pause_video"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_stop", F.when(F.col("action_type") == F.lit("stop_video"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_seek", F.when(F.col("action_type") == F.lit("seek_video"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "is_speed_change",
            F.when(F.col("action_type") == F.lit("speed_change_video"), F.lit(1)).otherwise(F.lit(0)),
        )
    )
    return base


def _base_metrics(base: DataFrame) -> dict[str, Column]:
    return {
        "first_event_time_utc": F.min("event_time_utc"),
        "last_event_time_utc": F.max("event_time_utc"),
        "event_count": F.count(F.lit(1)).cast("long"),
        "load_count": F.sum("is_load").cast("long"),
        "play_count": F.sum("is_play").cast("long"),
        "pause_count": F.sum("is_pause").cast("long"),
        "stop_count": F.sum("is_stop").cast("long"),
        "seek_count": F.sum("is_seek").cast("long"),
        "speed_change_count": F.sum("is_speed_change").cast("long"),
        "session_count": F.countDistinct("session_id").cast("long"),
        "video_length_s": F.max("duration_s").cast("double"),
        "max_watch_ratio": F.max("watch_ratio").cast("double"),
        "avg_watch_ratio": F.avg("watch_ratio").cast("double"),
        "max_position_s": F.max("current_time_s").cast("double"),
        "avg_position_s": F.avg("current_time_s").cast("double"),
        "seek_forward_count": F.sum(F.when(F.col("seek_direction") == F.lit("forward"), F.lit(1)).otherwise(F.lit(0))).cast("long"),
        "seek_backward_count": F.sum(F.when(F.col("seek_direction") == F.lit("backward"), F.lit(1)).otherwise(F.lit(0))).cast("long"),
        "avg_seek_distance_s": F.avg("seek_distance_s").cast("double"),
    }


def _aggregate(base: DataFrame, group_keys: list[str], metrics: dict[str, Column]) -> DataFrame:
    return base.groupBy(*group_keys).agg(*[expr.alias(name) for name, expr in metrics.items()])


def build_gold_user_video_engagement(base: DataFrame) -> DataFrame:
    group_keys = ["user_id", "course_id", "video_id", "video_block_id", "video_code"]
    return _aggregate(base, group_keys, _base_metrics(base)).withColumn(
        "completed_flag", F.when(F.col("max_watch_ratio") >= F.lit(0.9), F.lit(1)).otherwise(F.lit(0))
    )


def build_gold_user_video_engagement_daily(base: DataFrame) -> DataFrame:
    group_keys = ["event_date", "user_id", "course_id", "video_id", "video_block_id", "video_code"]
    return _aggregate(base, group_keys, _base_metrics(base)).withColumn(
        "completed_flag", F.when(F.col("max_watch_ratio") >= F.lit(0.9), F.lit(1)).otherwise(F.lit(0))
    )


def build_gold_course_video_summary_daily(base: DataFrame) -> DataFrame:
    group_keys = ["event_date", "course_id", "video_id", "video_block_id", "video_code"]
    summary = (
        base.groupBy(*group_keys)
        .agg(
            F.countDistinct("user_id").cast("long").alias("active_users"),
            F.countDistinct("session_id").cast("long").alias("session_count"),
            F.count(F.lit(1)).cast("long").alias("event_count"),
            F.sum("is_load").cast("long").alias("load_count"),
            F.sum("is_play").cast("long").alias("play_count"),
            F.sum("is_pause").cast("long").alias("pause_count"),
            F.sum("is_stop").cast("long").alias("stop_count"),
            F.sum("is_seek").cast("long").alias("seek_count"),
            F.sum("is_speed_change").cast("long").alias("speed_change_count"),
            F.max("duration_s").cast("double").alias("video_length_s"),
            F.avg("watch_ratio").cast("double").alias("avg_watch_ratio"),
            F.max("watch_ratio").cast("double").alias("max_watch_ratio"),
            F.max("current_time_s").cast("double").alias("max_position_s"),
            F.countDistinct(F.when(F.col("watch_ratio") >= F.lit(0.9), F.col("user_id"))).cast("long").alias("completed_users"),
            F.countDistinct(F.when(F.col("action_type") == F.lit("seek_video"), F.col("user_id"))).cast("long").alias("seek_users"),
        )
        .withColumn(
            "completion_rate",
            F.when(F.col("active_users") > 0, F.col("completed_users") / F.col("active_users")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "seek_rate",
            F.when(F.col("active_users") > 0, F.col("seek_users") / F.col("active_users")).otherwise(F.lit(0.0)),
        )
        .withColumn("dropoff_rate", F.lit(1.0) - F.col("completion_rate"))
    )
    return summary


def build_gold_course_video_seek_hotspots_daily(base: DataFrame) -> DataFrame:
    seeks = base.filter(F.col("action_type") == F.lit("seek_video")).filter(F.col("position_bucket_30s").isNotNull())
    group_keys = ["event_date", "course_id", "video_id", "video_block_id", "position_bucket_30s", "video_code"]
    return (
        seeks.groupBy(*group_keys)
        .agg(
            F.count(F.lit(1)).cast("long").alias("seek_event_count"),
            F.countDistinct("user_id").cast("long").alias("unique_seek_users"),
            F.sum(F.when(F.col("seek_direction") == F.lit("forward"), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("seek_forward_count"),
            F.sum(F.when(F.col("seek_direction") == F.lit("backward"), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("seek_backward_count"),
            F.avg("seek_distance_s").cast("double").alias("avg_seek_distance_s"),
            F.max("seek_distance_s").cast("double").alias("max_seek_distance_s"),
        )
    )


def build_gold_video_retention_by_bucket_daily(base: DataFrame) -> DataFrame:
    bucket_key = ["event_date", "course_id", "video_id", "video_block_id", "video_code", "position_bucket_30s"]
    user_progress = (
        base.groupBy("event_date", "course_id", "video_id", "video_block_id", "video_code", "user_id")
        .agg(F.max("position_bucket_30s").cast("long").alias("max_bucket_30s"))
    )
    total_users = (
        user_progress.groupBy("event_date", "course_id", "video_id", "video_block_id", "video_code")
        .agg(F.countDistinct("user_id").cast("long").alias("total_users"))
    )
    bucket_values = base.select(*bucket_key).dropDuplicates().filter(F.col("position_bucket_30s").isNotNull())
    bucket_reach = (
        bucket_values.alias("b")
        .join(
            user_progress.alias("u"),
            on=(
                (F.col("b.event_date") == F.col("u.event_date"))
                & (F.col("b.course_id") == F.col("u.course_id"))
                & (F.col("b.video_id") == F.col("u.video_id"))
                & (F.col("b.video_block_id") == F.col("u.video_block_id"))
                & (F.col("b.video_code") == F.col("u.video_code"))
                & (F.col("u.max_bucket_30s") >= F.col("b.position_bucket_30s"))
            ),
            how="inner",
        )
        .select(
            F.col("b.event_date").alias("event_date"),
            F.col("b.course_id").alias("course_id"),
            F.col("b.video_id").alias("video_id"),
            F.col("b.video_block_id").alias("video_block_id"),
            F.col("b.video_code").alias("video_code"),
            F.col("b.position_bucket_30s").alias("position_bucket_30s"),
            F.col("u.user_id").alias("user_id"),
        )
        .groupBy(*bucket_key)
        .agg(F.countDistinct("user_id").cast("long").alias("users_reached_bucket"))
    )
    bucket_behavior = (
        base.groupBy(*bucket_key)
        .agg(
            F.count(F.lit(1)).cast("long").alias("event_count"),
            F.countDistinct(F.when(F.col("action_type") == F.lit("pause_video"), F.col("user_id"))).cast("long").alias("users_paused_here"),
            F.countDistinct(F.when(F.col("action_type") == F.lit("stop_video"), F.col("user_id"))).cast("long").alias("users_stopped_here"),
            F.countDistinct(F.when(F.col("action_type") == F.lit("seek_video"), F.col("user_id"))).cast("long").alias("users_seeked_here"),
            F.sum(F.when(F.col("action_type") == F.lit("pause_video"), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("pause_event_count"),
            F.sum(F.when(F.col("action_type") == F.lit("stop_video"), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("stop_event_count"),
            F.sum(F.when(F.col("action_type") == F.lit("seek_video"), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("seek_event_count"),
        )
    )
    return (
        bucket_behavior.join(bucket_reach, on=bucket_key, how="left")
        .join(total_users, on=["event_date", "course_id", "video_id", "video_block_id", "video_code"], how="left")
        .withColumn("users_reached_bucket", F.coalesce(F.col("users_reached_bucket"), F.lit(0)).cast("long"))
        .withColumn(
            "retention_rate",
            F.when(F.col("total_users") > 0, F.col("users_reached_bucket") / F.col("total_users")).otherwise(F.lit(0.0)),
        )
    )
