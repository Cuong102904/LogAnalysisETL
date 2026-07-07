from __future__ import annotations

from dataclasses import dataclass

from projects.daotao_ai.gold.config_loader import env_str, load_app_config


@dataclass(frozen=True)
class VideoGoldConfig:
    app_name: str
    input_video_events_path: str
    output_user_video_engagement_path: str
    output_user_video_engagement_daily_path: str
    output_course_video_summary_daily_path: str
    output_course_video_seek_hotspots_daily_path: str
    output_video_retention_by_bucket_daily_path: str
    query_name: str

    @classmethod
    def from_env(cls) -> VideoGoldConfig:
        config = load_app_config("GOLD_VIDEO_CONFIG_PATH", "gold_video_batch.yaml")
        return cls(
            app_name=env_str(
                "GOLD_VIDEO_APP_NAME",
                config,
                "app_name",
                default="gold_video_batch",
            ),
            input_video_events_path=env_str(
                "SILVER_VIDEO_EVENTS_PATH",
                config,
                "input",
                "silver_video_events_path",
                default="s3a://lakehouse/learnlake/silver/video_interactions",
            ),
            output_user_video_engagement_path=env_str(
                "GOLD_USER_VIDEO_ENGAGEMENT_PATH",
                config,
                "storage",
                "gold_user_video_engagement_path",
                default="s3a://lakehouse/learnlake/gold/gold_user_video_engagement",
            ),
            output_user_video_engagement_daily_path=env_str(
                "GOLD_USER_VIDEO_ENGAGEMENT_DAILY_PATH",
                config,
                "storage",
                "gold_user_video_engagement_daily_path",
                default="s3a://lakehouse/learnlake/gold/gold_user_video_engagement_daily",
            ),
            output_course_video_summary_daily_path=env_str(
                "GOLD_COURSE_VIDEO_SUMMARY_DAILY_PATH",
                config,
                "storage",
                "gold_course_video_summary_daily_path",
                default="s3a://lakehouse/learnlake/gold/gold_course_video_summary_daily",
            ),
            output_course_video_seek_hotspots_daily_path=env_str(
                "GOLD_COURSE_VIDEO_SEEK_HOTSPOTS_DAILY_PATH",
                config,
                "storage",
                "gold_course_video_seek_hotspots_daily_path",
                default="s3a://lakehouse/learnlake/gold/gold_course_video_seek_hotspots_daily",
            ),
            output_video_retention_by_bucket_daily_path=env_str(
                "GOLD_VIDEO_RETENTION_BY_BUCKET_DAILY_PATH",
                config,
                "storage",
                "gold_video_retention_by_bucket_daily_path",
                default="s3a://lakehouse/learnlake/gold/gold_video_retention_by_bucket_daily",
            ),
            query_name=env_str(
                "GOLD_VIDEO_QUERY_NAME",
                config,
                "streaming",
                "query_name",
                default="gold_video_batch",
            ),
        )
