from dataclasses import dataclass


@dataclass(frozen=True)
class GoldConfig:
    app_name: str = "gold_aggregator"
    input_video_path: str = "s3a://silver/mooc/silver/video_interactions"
    output_video_features_path: str = "s3a://gold/mooc/gold/video_anomaly_features"
    checkpoint_path: str = "s3a://checkpoints/mooc/gold_aggregator/video_anomaly"
    bucket_seconds: int = 5
