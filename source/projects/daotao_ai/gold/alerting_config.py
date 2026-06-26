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
            input_behavior_anomaly_signals_path=_path_value(
                config,
                ("GOLD_BEHAVIOR_ANOMALY_SIGNALS_PATH",),
                ("input", "behavior_anomaly_signals_path"),
                "s3a://lakehouse/learnlake/gold/behavior_anomaly_signals",
            ),
            output_anomaly_alerts_path=_path_value(
                config,
                ("GOLD_ANOMALY_ALERTS_PATH",),
                ("storage", "anomaly_alerts_path"),
                "s3a://lakehouse/learnlake/gold/anomaly_alerts",
            ),
            checkpoint_base=_path_value(
                config,
                ("GOLD_ALERTING_CHECKPOINT_BASE",),
                ("checkpoint", "base_path"),
                "s3a://platform/learnlake/gold_alerting",
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
