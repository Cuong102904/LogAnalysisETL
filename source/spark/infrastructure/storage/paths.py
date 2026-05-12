from dataclasses import dataclass


@dataclass(frozen=True)
class TablePaths:
    bronze_raw: str
    silver_learning: str
    silver_performance: str
    silver_system: str
    silver_unknown: str
    silver_video_interactions: str
    gold_video_anomaly_features: str
