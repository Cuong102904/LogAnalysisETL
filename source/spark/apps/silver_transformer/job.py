from domain.silver.classifier import add_classification
from domain.silver.normalizers.exam_attempts import normalize_exam_attempts
from domain.silver.normalizers.learning import normalize_learning
from domain.silver.normalizers.navigation import normalize_navigation
from domain.silver.normalizers.pdf_interactions import normalize_pdf_interactions
from domain.silver.normalizers.performance import normalize_performance
from domain.silver.normalizers.system import normalize_system
from domain.silver.normalizers.unknown import normalize_unknown
from domain.silver.normalizers.video_interaction import normalize_video_interactions
from infrastructure.spark.session import build_spark

from apps.silver_transformer.config import SilverConfig


def _write_stream(df, path: str, checkpoint: str, query_name: str) -> None:
    (
        df.writeStream.format("delta")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .queryName(query_name)
        .start()
    )


def run(config: SilverConfig) -> None:
    spark = build_spark(config.app_name)
    bronze = spark.readStream.format("delta").load(config.input_path)
    classified = add_classification(bronze)

    # Base table: all authenticated user events
    learning = normalize_learning(classified)

    # Domain-specific tables (read from classified, not from learning)
    performance = normalize_performance(classified)
    exam = normalize_exam_attempts(classified)
    video = normalize_video_interactions(learning)  # filter from learning (has event_json)
    navigation = normalize_navigation(classified)
    pdf = normalize_pdf_interactions(classified)
    system = normalize_system(classified)
    unknown = normalize_unknown(classified)

    _write_stream(
        learning,
        config.learning_path,
        f"{config.checkpoint_base}/learning",
        config.learning_query_name,
    )
    _write_stream(
        performance,
        config.performance_path,
        f"{config.checkpoint_base}/performance",
        config.performance_query_name,
    )
    _write_stream(exam, config.exam_path, f"{config.checkpoint_base}/exam", config.exam_query_name)
    _write_stream(
        video, config.video_path, f"{config.checkpoint_base}/video", config.video_query_name
    )
    _write_stream(
        navigation,
        config.navigation_path,
        f"{config.checkpoint_base}/navigation",
        config.navigation_query_name,
    )
    _write_stream(pdf, config.pdf_path, f"{config.checkpoint_base}/pdf", config.pdf_query_name)
    _write_stream(
        system, config.system_path, f"{config.checkpoint_base}/system", config.system_query_name
    )
    _write_stream(
        unknown, config.unknown_path, f"{config.checkpoint_base}/unknown", config.unknown_query_name
    )

    spark.streams.awaitAnyTermination()
