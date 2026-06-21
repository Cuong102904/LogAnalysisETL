from dataclasses import dataclass

from projects.daotao_ai.gold.config_loader import env_int, env_str, load_app_config, required_env_str


@dataclass(frozen=True)
class GoldAlertingConfig:
    app_name: str
    input_behavior_anomaly_signals_path: str
    output_anomaly_alerts_path: str
    checkpoint_base: str
    query_name: str
    trigger_interval_seconds: int
    min_event_count: int
    min_distinct_users: int
    min_z_score: float

    @classmethod
    def from_env(cls) -> "GoldAlertingConfig":
        config = load_app_config("GOLD_ALERTING_CONFIG_PATH", "gold_course_activity_summary.yaml")
        return cls(
            app_name=env_str("GOLD_ALERTING_APP_NAME", config, "app_name", default="gold_alerting"),
            input_behavior_anomaly_signals_path=required_env_str(
                "GOLD_BEHAVIOR_ANOMALY_SIGNALS_PATH",
                config,
                "input",
                "behavior_anomaly_signals_path",
            ),
            output_anomaly_alerts_path=required_env_str(
                "GOLD_ANOMALY_ALERTS_PATH",
                config,
                "storage",
                "anomaly_alerts_path",
            ),
            checkpoint_base=required_env_str(
                "GOLD_ALERTING_CHECKPOINT_BASE",
                config,
                "checkpoint",
                "base_path",
            ),
            query_name=env_str(
                "GOLD_ALERTING_QUERY_NAME",
                config,
                "streaming",
                "query_name",
                default="gold_anomaly_alerts",
            ),
            trigger_interval_seconds=env_int(
                "GOLD_ALERTING_TRIGGER_INTERVAL_SECONDS",
                config,
                "streaming",
                "trigger_interval_seconds",
                default=30,
            ),
            min_event_count=env_int(
                "GOLD_ALERTING_MIN_EVENT_COUNT",
                config,
                "rules",
                "min_event_count",
                default=3,
            ),
            min_distinct_users=env_int(
                "GOLD_ALERTING_MIN_DISTINCT_USERS",
                config,
                "rules",
                "min_distinct_users",
                default=2,
            ),
            min_z_score=float(
                env_str(
                    "GOLD_ALERTING_MIN_Z_SCORE",
                    config,
                    "rules",
                    "min_z_score",
                    default="2.0",
                )
            ),
        )
