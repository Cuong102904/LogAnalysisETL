from dataclasses import dataclass

from apps.config_utils import env_int, env_str, load_app_config, required_env_str


@dataclass(frozen=True)
class GoldConfig:
    app_name: str
    input_video_path: str
    output_video_features_path: str
    checkpoint_path: str
    query_name: str
    bucket_seconds: int

    @classmethod
    def from_env(cls) -> "GoldConfig":
        config = load_app_config("GOLD_CONFIG_PATH", "gold_aggregator.yaml")
        return cls(
            app_name=env_str("GOLD_APP_NAME", config, "app_name", default="gold_aggregator"),
            input_video_path=required_env_str(
                "SILVER_VIDEO_INTERACTIONS_PATH",
                config,
                "input",
                "silver_video_interactions_path",
            ),
            output_video_features_path=required_env_str(
                "GOLD_VIDEO_ANOMALY_FEATURES_PATH",
                config,
                "storage",
                "video_anomaly_features_path",
            ),
            checkpoint_path=required_env_str(
                "GOLD_VIDEO_ANOMALY_CHECKPOINT_PATH",
                config,
                "checkpoint",
                "video_anomaly_path",
            ),
            query_name=env_str(
                "GOLD_VIDEO_ANOMALY_QUERY_NAME",
                config,
                "streaming",
                "video_anomaly_query_name",
                default="gold_video_anomaly_features",
            ),
            bucket_seconds=env_int(
                "GOLD_VIDEO_BUCKET_SECONDS", config, "options", "bucket_seconds", default=5
            ),
        )
