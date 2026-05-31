from apps.gold_aggregator.config import GoldConfig
from domain.gold.behavior_anomalies.aggregator import build_behavior_anomalies
from domain.gold.exam_anomaly.aggregator import build_exam_anomaly_features
from domain.gold.learning_journey.aggregator import build_learning_journey_features
from domain.gold.pdf_behavior.aggregator import build_pdf_behavior_features
from domain.gold.quiz_performance.aggregator import build_quiz_performance_features
from domain.gold.video_anomaly.aggregator import build_video_anomaly_features
from infrastructure.spark.session import build_spark


def _start_stream(
    df,
    path: str,
    checkpoint: str,
    query_name: str,
    trigger_seconds: int,
    partition_by: list[str] | None = None,
):
    writer = (
        df.writeStream.format("delta")
        .outputMode("complete")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .queryName(query_name)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    return writer.start()


def run(config: GoldConfig) -> None:
    spark = build_spark(config.app_name)
    learning = spark.readStream.format("delta").load(config.input_learning_path)
    video = spark.readStream.format("delta").load(config.input_video_path)
    pdf = spark.readStream.format("delta").load(config.input_pdf_path)
    performance = spark.readStream.format("delta").load(config.input_performance_path)
    exam_attempts = spark.readStream.format("delta").load(config.input_exam_attempts_path)
    system = spark.readStream.format("delta").load(config.input_system_path)

    video_friction_signals = build_video_anomaly_features(video, config.bucket_seconds)
    pdf_engagement_features = build_pdf_behavior_features(pdf)
    quiz_attempt_metrics = build_quiz_performance_features(performance)
    user_learning_profile_daily = build_learning_journey_features(learning)
    exam_integrity_signals = build_exam_anomaly_features(exam_attempts, system)
    behavior_anomaly_signals = build_behavior_anomalies(
        video,
        pdf,
        performance,
        learning,
    )

    _start_stream(
        video_friction_signals,
        config.output_video_friction_signals_path,
        f"{config.checkpoint_base}/video_friction_signals",
        config.video_query_name,
        config.trigger_interval_seconds,
        ["event_date", "course_id"],
    )
    _start_stream(
        pdf_engagement_features,
        config.output_pdf_engagement_features_path,
        f"{config.checkpoint_base}/pdf_engagement_features",
        config.pdf_query_name,
        config.trigger_interval_seconds,
        ["event_date", "course_id"],
    )
    _start_stream(
        quiz_attempt_metrics,
        config.output_quiz_attempt_metrics_path,
        f"{config.checkpoint_base}/quiz_attempt_metrics",
        config.quiz_query_name,
        config.trigger_interval_seconds,
        ["event_date", "course_id"],
    )
    _start_stream(
        user_learning_profile_daily,
        config.output_user_learning_profile_daily_path,
        f"{config.checkpoint_base}/user_learning_profile_daily",
        config.journey_query_name,
        config.trigger_interval_seconds,
        ["event_date", "course_id"],
    )
    _start_stream(
        exam_integrity_signals,
        config.output_exam_integrity_signals_path,
        f"{config.checkpoint_base}/exam_integrity_signals",
        config.exam_query_name,
        config.trigger_interval_seconds,
        ["event_date", "course_id"],
    )
    _start_stream(
        behavior_anomaly_signals,
        config.output_behavior_anomaly_signals_path,
        f"{config.checkpoint_base}/behavior_anomaly_signals",
        config.anomaly_query_name,
        config.trigger_interval_seconds,
        ["event_date", "anomaly_domain"],
    )

    spark.streams.awaitAnyTermination()

