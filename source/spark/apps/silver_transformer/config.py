from dataclasses import dataclass


@dataclass(frozen=True)
class SilverConfig:
    app_name: str = "silver_transformer"
    input_path: str = "s3a://bronze/mooc/bronze/mooc_events_raw"
    learning_path: str = "s3a://silver/mooc/silver/learning_events"
    performance_path: str = "s3a://silver/mooc/silver/performance_events"
    system_path: str = "s3a://silver/mooc/silver/system_events"
    unknown_path: str = "s3a://silver/mooc/silver/unknown_events"
    video_path: str = "s3a://silver/mooc/silver/video_interactions"
    checkpoint_base: str = "s3a://checkpoints/mooc/silver_transformer"
