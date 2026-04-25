from apps.silver_transformer.config import SilverConfig
from domain.silver.classifier import add_classification
from domain.silver.normalizers.learning import normalize_learning
from domain.silver.normalizers.performance import normalize_performance
from domain.silver.normalizers.system import normalize_system
from domain.silver.normalizers.unknown import normalize_unknown
from domain.silver.normalizers.video_interaction import normalize_video_interactions
from infrastructure.spark.session import build_spark


def _start_delta_writer(df, path: str, checkpoint: str) -> None:
    (
        df.writeStream.format("delta")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )


def run(config: SilverConfig) -> None:
    spark = build_spark(config.app_name)
    bronze = spark.readStream.format("delta").load(config.input_path)
    classified = add_classification(bronze)

    learning = normalize_learning(classified)
    performance = normalize_performance(classified)
    system = normalize_system(classified)
    unknown = normalize_unknown(classified)
    video = normalize_video_interactions(learning)

    _start_delta_writer(learning, config.learning_path, f"{config.checkpoint_base}/learning")
    _start_delta_writer(performance, config.performance_path, f"{config.checkpoint_base}/performance")
    _start_delta_writer(system, config.system_path, f"{config.checkpoint_base}/system")
    _start_delta_writer(unknown, config.unknown_path, f"{config.checkpoint_base}/unknown")
    _start_delta_writer(video, config.video_path, f"{config.checkpoint_base}/video")
    spark.streams.awaitAnyTermination()
