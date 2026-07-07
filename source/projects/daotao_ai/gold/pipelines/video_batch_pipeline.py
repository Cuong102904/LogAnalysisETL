from __future__ import annotations

from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from learnlake.runtime import build_spark
from projects.daotao_ai.gold.domain.video import (
    build_gold_course_video_seek_hotspots_daily,
    build_gold_course_video_summary_daily,
    build_gold_user_video_engagement,
    build_gold_user_video_engagement_daily,
    build_gold_video_retention_by_bucket_daily,
    build_video_events_base,
)
from projects.daotao_ai.gold.video_config import VideoGoldConfig


def _merge_condition(keys: list[str]) -> str:
    return " AND ".join(f"t.{key} <=> s.{key}" for key in keys)


def _upsert_delta(
    df: DataFrame,
    path: str,
    merge_keys: list[str],
    *,
    partition_by: str | None = None,
) -> None:
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, path):
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by is not None:
            writer = writer.partitionBy(partition_by)
        writer.save(path)
        return

    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(df.alias("s"), _merge_condition(merge_keys))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _read_delta(spark, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def _filter_by_range(df: DataFrame, start_date: date | None, end_date: date | None) -> DataFrame:
    filtered = df
    if start_date is not None:
        filtered = filtered.filter(F.col("event_date") >= F.lit(start_date.isoformat()))
    if end_date is not None:
        filtered = filtered.filter(F.col("event_date") <= F.lit(end_date.isoformat()))
    return filtered


def _parse_date(value: str | None) -> date | None:
    if value in (None, ""):
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def run(config: VideoGoldConfig, *, start_date: str | None = None, end_date: str | None = None) -> None:
    spark = build_spark(config.app_name)
    range_start = _parse_date(start_date)
    range_end = _parse_date(end_date)

    silver_df = _read_delta(spark, config.input_video_events_path)
    base_df = build_video_events_base(silver_df)
    filtered_base_df = _filter_by_range(base_df, range_start, range_end)

    user_video_snapshot_df = build_gold_user_video_engagement(base_df)
    user_video_daily_df = build_gold_user_video_engagement_daily(filtered_base_df)
    course_summary_daily_df = build_gold_course_video_summary_daily(filtered_base_df)
    seek_hotspots_daily_df = build_gold_course_video_seek_hotspots_daily(filtered_base_df)
    retention_daily_df = build_gold_video_retention_by_bucket_daily(filtered_base_df)

    _upsert_delta(
        user_video_snapshot_df,
        config.output_user_video_engagement_path,
        ["user_id", "course_id", "video_id", "video_block_id", "video_code"],
    )
    _upsert_delta(
        user_video_daily_df,
        config.output_user_video_engagement_daily_path,
        ["event_date", "user_id", "course_id", "video_id", "video_block_id", "video_code"],
        partition_by="event_date",
    )
    _upsert_delta(
        course_summary_daily_df,
        config.output_course_video_summary_daily_path,
        ["event_date", "course_id", "video_id", "video_block_id", "video_code"],
        partition_by="event_date",
    )
    _upsert_delta(
        seek_hotspots_daily_df,
        config.output_course_video_seek_hotspots_daily_path,
        ["event_date", "course_id", "video_id", "video_block_id", "video_code", "position_bucket_30s"],
        partition_by="event_date",
    )
    _upsert_delta(
        retention_daily_df,
        config.output_video_retention_by_bucket_daily_path,
        ["event_date", "course_id", "video_id", "video_block_id", "video_code", "position_bucket_30s"],
        partition_by="event_date",
    )

    print(
        f"{config.query_name} wrote "
        f"user_video={config.output_user_video_engagement_path} "
        f"user_video_daily={config.output_user_video_engagement_daily_path} "
        f"summary_daily={config.output_course_video_summary_daily_path} "
        f"seek_hotspots_daily={config.output_course_video_seek_hotspots_daily_path} "
        f"retention_daily={config.output_video_retention_by_bucket_daily_path} "
        f"range={range_start}..{range_end}",
        flush=True,
    )
