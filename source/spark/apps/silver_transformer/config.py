from dataclasses import dataclass

from apps.config_utils import env_str, load_app_config, required_env_str


@dataclass(frozen=True)
class SilverConfig:
    app_name: str
    input_path: str
    learning_path: str
    performance_path: str
    system_path: str
    unknown_path: str
    video_path: str
    checkpoint_base: str
    learning_query_name: str
    performance_query_name: str
    system_query_name: str
    unknown_query_name: str
    video_query_name: str

    @classmethod
    def from_env(cls) -> "SilverConfig":
        config = load_app_config("SILVER_CONFIG_PATH", "silver_transformer.yaml")
        return cls(
            app_name=env_str("SILVER_APP_NAME", config, "app_name", default="silver_transformer"),
            input_path=required_env_str(
                "BRONZE_TABLE_PATH",
                config,
                "input",
                "bronze_table_path",
            ),
            learning_path=required_env_str(
                "SILVER_LEARNING_PATH",
                config,
                "storage",
                "learning_path",
            ),
            performance_path=required_env_str(
                "SILVER_PERFORMANCE_PATH",
                config,
                "storage",
                "performance_path",
            ),
            system_path=required_env_str(
                "SILVER_SYSTEM_PATH",
                config,
                "storage",
                "system_path",
            ),
            unknown_path=required_env_str(
                "SILVER_UNKNOWN_PATH",
                config,
                "storage",
                "unknown_path",
            ),
            video_path=required_env_str(
                "SILVER_VIDEO_INTERACTIONS_PATH",
                config,
                "storage",
                "video_interactions_path",
            ),
            checkpoint_base=required_env_str(
                "SILVER_CHECKPOINT_BASE",
                config,
                "checkpoint",
                "base_path",
            ),
            learning_query_name=env_str(
                "SILVER_LEARNING_QUERY_NAME",
                config,
                "streaming",
                "learning_query_name",
                default="silver_learning_events",
            ),
            performance_query_name=env_str(
                "SILVER_PERFORMANCE_QUERY_NAME",
                config,
                "streaming",
                "performance_query_name",
                default="silver_performance_events",
            ),
            system_query_name=env_str(
                "SILVER_SYSTEM_QUERY_NAME",
                config,
                "streaming",
                "system_query_name",
                default="silver_system_events",
            ),
            unknown_query_name=env_str(
                "SILVER_UNKNOWN_QUERY_NAME",
                config,
                "streaming",
                "unknown_query_name",
                default="silver_unknown_events",
            ),
            video_query_name=env_str(
                "SILVER_VIDEO_QUERY_NAME",
                config,
                "streaming",
                "video_query_name",
                default="silver_video_interactions",
            ),
        )
