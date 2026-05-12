from dataclasses import dataclass

from apps.config_utils import env_str, load_app_config, required_env_str


@dataclass(frozen=True)
class SilverConfig:
    app_name: str
    input_path: str
    # Silver table paths
    learning_path: str
    performance_path: str
    exam_path: str
    video_path: str
    navigation_path: str
    pdf_path: str
    system_path: str
    unknown_path: str
    # Checkpoint
    checkpoint_base: str
    # Query names
    learning_query_name: str
    performance_query_name: str
    exam_query_name: str
    video_query_name: str
    navigation_query_name: str
    pdf_query_name: str
    system_query_name: str
    unknown_query_name: str

    @classmethod
    def from_env(cls) -> "SilverConfig":
        config = load_app_config("SILVER_CONFIG_PATH", "silver_transformer.yaml")
        return cls(
            app_name=env_str("SILVER_APP_NAME", config, "app_name", default="silver_transformer"),
            input_path=required_env_str("BRONZE_TABLE_PATH", config, "input", "bronze_table_path"),
            # paths
            learning_path=required_env_str("SILVER_LEARNING_PATH", config, "storage", "learning_path"),
            performance_path=required_env_str("SILVER_PERFORMANCE_PATH", config, "storage", "performance_path"),
            exam_path=required_env_str("SILVER_EXAM_PATH", config, "storage", "exam_path"),
            video_path=required_env_str("SILVER_VIDEO_PATH", config, "storage", "video_path"),
            navigation_path=required_env_str("SILVER_NAVIGATION_PATH", config, "storage", "navigation_path"),
            pdf_path=required_env_str("SILVER_PDF_PATH", config, "storage", "pdf_path"),
            system_path=required_env_str("SILVER_SYSTEM_PATH", config, "storage", "system_path"),
            unknown_path=required_env_str("SILVER_UNKNOWN_PATH", config, "storage", "unknown_path"),
            # checkpoint
            checkpoint_base=required_env_str("SILVER_CHECKPOINT_BASE", config, "checkpoint", "base_path"),
            # query names
            learning_query_name=env_str("SILVER_LEARNING_QUERY_NAME", config, "streaming", "learning_query_name", default="silver_learning_events"),
            performance_query_name=env_str("SILVER_PERFORMANCE_QUERY_NAME", config, "streaming", "performance_query_name", default="silver_performance_events"),
            exam_query_name=env_str("SILVER_EXAM_QUERY_NAME", config, "streaming", "exam_query_name", default="silver_exam_attempts"),
            video_query_name=env_str("SILVER_VIDEO_QUERY_NAME", config, "streaming", "video_query_name", default="silver_video_interactions"),
            navigation_query_name=env_str("SILVER_NAVIGATION_QUERY_NAME", config, "streaming", "navigation_query_name", default="silver_navigation_events"),
            pdf_query_name=env_str("SILVER_PDF_QUERY_NAME", config, "streaming", "pdf_query_name", default="silver_pdf_interactions"),
            system_query_name=env_str("SILVER_SYSTEM_QUERY_NAME", config, "streaming", "system_query_name", default="silver_system_events"),
            unknown_query_name=env_str("SILVER_UNKNOWN_QUERY_NAME", config, "streaming", "unknown_query_name", default="silver_unknown_events"),
        )
