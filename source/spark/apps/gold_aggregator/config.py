from dataclasses import dataclass

from shared.config_loader import env_int, env_str, load_app_config, required_env_str


@dataclass(frozen=True)
class GoldConfig:
    app_name: str
    input_learning_path: str
    input_video_path: str
    input_pdf_path: str
    input_performance_path: str
    input_exam_attempts_path: str
    input_system_path: str
    output_video_friction_signals_path: str
    output_pdf_engagement_features_path: str
    output_quiz_attempt_metrics_path: str
    output_user_learning_profile_daily_path: str
    output_exam_integrity_signals_path: str
    output_behavior_anomaly_signals_path: str
    checkpoint_base: str
    video_query_name: str
    pdf_query_name: str
    quiz_query_name: str
    journey_query_name: str
    exam_query_name: str
    anomaly_query_name: str
    bucket_seconds: int
    trigger_interval_seconds: int

    @classmethod
    def from_env(cls) -> "GoldConfig":
        config = load_app_config("GOLD_CONFIG_PATH", "gold_aggregator.yaml")
        return cls(
            app_name=env_str("GOLD_APP_NAME", config, "app_name", default="gold_aggregator"),
            input_learning_path=required_env_str(
                "SILVER_LEARNING_PATH",
                config,
                "input",
                "silver_learning_path",
            ),
            input_video_path=required_env_str(
                "SILVER_VIDEO_INTERACTIONS_PATH",
                config,
                "input",
                "silver_video_interactions_path",
            ),
            input_pdf_path=required_env_str(
                "SILVER_PDF_PATH",
                config,
                "input",
                "silver_pdf_path",
            ),
            input_performance_path=required_env_str(
                "SILVER_PERFORMANCE_PATH",
                config,
                "input",
                "silver_performance_path",
            ),
            input_exam_attempts_path=required_env_str(
                "SILVER_EXAM_ATTEMPTS_PATH",
                config,
                "input",
                "silver_exam_attempts_path",
            ),
            input_system_path=required_env_str(
                "SILVER_SYSTEM_PATH",
                config,
                "input",
                "silver_system_path",
            ),
            output_video_friction_signals_path=required_env_str(
                "GOLD_VIDEO_FRICTION_SIGNALS_PATH",
                config,
                "storage",
                "video_friction_signals_path",
            ),
            output_pdf_engagement_features_path=required_env_str(
                "GOLD_PDF_ENGAGEMENT_FEATURES_PATH",
                config,
                "storage",
                "pdf_engagement_features_path",
            ),
            output_quiz_attempt_metrics_path=required_env_str(
                "GOLD_QUIZ_ATTEMPT_METRICS_PATH",
                config,
                "storage",
                "quiz_attempt_metrics_path",
            ),
            output_user_learning_profile_daily_path=required_env_str(
                "GOLD_USER_LEARNING_PROFILE_DAILY_PATH",
                config,
                "storage",
                "user_learning_profile_daily_path",
            ),
            output_exam_integrity_signals_path=required_env_str(
                "GOLD_EXAM_INTEGRITY_SIGNALS_PATH",
                config,
                "storage",
                "exam_integrity_signals_path",
            ),
            output_behavior_anomaly_signals_path=required_env_str(
                "GOLD_BEHAVIOR_ANOMALY_SIGNALS_PATH",
                config,
                "storage",
                "behavior_anomaly_signals_path",
            ),
            checkpoint_base=required_env_str(
                "GOLD_CHECKPOINT_BASE",
                config,
                "checkpoint",
                "base_path",
            ),
            video_query_name=env_str(
                "GOLD_VIDEO_FRICTION_SIGNALS_QUERY_NAME",
                config,
                "streaming",
                "video_friction_signals_query_name",
                default="gold_video_friction_signals",
            ),
            pdf_query_name=env_str(
                "GOLD_PDF_ENGAGEMENT_QUERY_NAME",
                config,
                "streaming",
                "pdf_engagement_query_name",
                default="gold_pdf_engagement_features",
            ),
            quiz_query_name=env_str(
                "GOLD_QUIZ_ATTEMPT_METRICS_QUERY_NAME",
                config,
                "streaming",
                "quiz_attempt_metrics_query_name",
                default="gold_quiz_attempt_metrics",
            ),
            journey_query_name=env_str(
                "GOLD_USER_LEARNING_PROFILE_DAILY_QUERY_NAME",
                config,
                "streaming",
                "user_learning_profile_daily_query_name",
                default="gold_user_learning_profile_daily",
            ),
            exam_query_name=env_str(
                "GOLD_EXAM_INTEGRITY_SIGNALS_QUERY_NAME",
                config,
                "streaming",
                "exam_integrity_signals_query_name",
                default="gold_exam_integrity_signals",
            ),
            anomaly_query_name=env_str(
                "GOLD_BEHAVIOR_ANOMALY_SIGNALS_QUERY_NAME",
                config,
                "streaming",
                "behavior_anomaly_signals_query_name",
                default="gold_behavior_anomaly_signals",
            ),
            bucket_seconds=env_int(
                "GOLD_VIDEO_BUCKET_SECONDS", config, "options", "bucket_seconds", default=5
            ),
            trigger_interval_seconds=env_int(
                "GOLD_TRIGGER_INTERVAL_SECONDS",
                config,
                "streaming",
                "trigger_interval_seconds",
                default=30,
            ),
        )
