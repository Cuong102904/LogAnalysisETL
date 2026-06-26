from dataclasses import dataclass
import os

from typing import Iterable

from projects.daotao_ai.gold.config_loader import env_int, env_str, load_app_config, nested_value


def _path_value(
    config: dict[str, object],
    env_names: Iterable[str],
    config_keys: tuple[str, ...],
    default: str,
) -> str:
    for env_name in env_names:
        value = os.getenv(env_name)
        if value:
            return value

    config_value = nested_value(config, *config_keys)
    if config_value is not None:
        return str(config_value)

    return default


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
        config = load_app_config("GOLD_CONFIG_PATH", "gold_course_activity_summary.yaml")
        return cls(
            app_name=env_str("GOLD_APP_NAME", config, "app_name", default="gold_aggregator"),
            input_learning_path=_path_value(
                config,
                ("SILVER_LEARNING_PATH", "SILVER_EVENT_INDEX_PATH"),
                ("input", "silver_learning_path"),
                "s3a://lakehouse/learnlake/silver/silver_event_index",
            ),
            input_video_path=_path_value(
                config,
                ("SILVER_VIDEO_PATH", "SILVER_VIDEO_INTERACTIONS_PATH"),
                ("input", "silver_video_interactions_path"),
                "s3a://lakehouse/learnlake/silver/silver_video_events",
            ),
            input_pdf_path=_path_value(
                config,
                ("SILVER_DOCUMENT_PATH", "SILVER_PDF_PATH"),
                ("input", "silver_pdf_path"),
                "s3a://lakehouse/learnlake/silver/silver_document_events",
            ),
            input_performance_path=_path_value(
                config,
                ("SILVER_ASSESSMENT_PATH", "SILVER_PERFORMANCE_PATH"),
                ("input", "silver_performance_path"),
                "s3a://lakehouse/learnlake/silver/silver_assessment_events",
            ),
            input_exam_attempts_path=_path_value(
                config,
                ("SILVER_EXAM_PATH", "SILVER_EXAM_ATTEMPTS_PATH"),
                ("input", "silver_exam_attempts_path"),
                "s3a://lakehouse/learnlake/silver/silver_exam_events",
            ),
            input_system_path=_path_value(
                config,
                ("SILVER_SYSTEM_PATH",),
                ("input", "silver_system_path"),
                "s3a://lakehouse/learnlake/silver/silver_system_events",
            ),
            output_video_friction_signals_path=_path_value(
                config,
                ("GOLD_VIDEO_FRICTION_SIGNALS_PATH",),
                ("storage", "video_friction_signals_path"),
                "s3a://lakehouse/learnlake/gold/video_friction_signals",
            ),
            output_pdf_engagement_features_path=_path_value(
                config,
                ("GOLD_PDF_ENGAGEMENT_FEATURES_PATH",),
                ("storage", "pdf_engagement_features_path"),
                "s3a://lakehouse/learnlake/gold/pdf_engagement_features",
            ),
            output_quiz_attempt_metrics_path=_path_value(
                config,
                ("GOLD_QUIZ_ATTEMPT_METRICS_PATH",),
                ("storage", "quiz_attempt_metrics_path"),
                "s3a://lakehouse/learnlake/gold/quiz_attempt_metrics",
            ),
            output_user_learning_profile_daily_path=_path_value(
                config,
                ("GOLD_USER_LEARNING_PROFILE_DAILY_PATH",),
                ("storage", "user_learning_profile_daily_path"),
                "s3a://lakehouse/learnlake/gold/user_learning_profile_daily",
            ),
            output_exam_integrity_signals_path=_path_value(
                config,
                ("GOLD_EXAM_INTEGRITY_SIGNALS_PATH",),
                ("storage", "exam_integrity_signals_path"),
                "s3a://lakehouse/learnlake/gold/exam_integrity_signals",
            ),
            output_behavior_anomaly_signals_path=_path_value(
                config,
                ("GOLD_BEHAVIOR_ANOMALY_SIGNALS_PATH",),
                ("storage", "behavior_anomaly_signals_path"),
                "s3a://lakehouse/learnlake/gold/behavior_anomaly_signals",
            ),
            checkpoint_base=_path_value(
                config,
                ("GOLD_CHECKPOINT_BASE",),
                ("checkpoint", "base_path"),
                "s3a://platform/learnlake/gold_aggregator",
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
