from apps.gold_aggregator.config import GoldConfig
from domain.gold.video_anomaly.aggregator import aggregate_features
from domain.gold.video_anomaly.time_bucketing import bucket_video_interactions
from infrastructure.spark.session import build_spark


def run(config: GoldConfig) -> None:
    spark = build_spark(config.app_name)
    video = spark.readStream.format("delta").load(config.input_video_path)
    bucketed = bucket_video_interactions(video, config.bucket_seconds)
    features = aggregate_features(bucketed)
    (
        features.writeStream.format("delta")
        .option("path", config.output_video_features_path)
        .option("checkpointLocation", config.checkpoint_path)
        .outputMode("append")
        .start()
        .awaitTermination()
    )
