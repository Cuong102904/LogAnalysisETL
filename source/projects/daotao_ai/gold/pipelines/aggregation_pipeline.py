from learnlake.runtime import build_spark
from projects.daotao_ai.gold.aggregation_config import GoldConfig
from projects.daotao_ai.gold.domain.behavior_anomalies.aggregator import build_behavior_anomalies
from projects.daotao_ai.gold.domain.exam_anomaly.aggregator import build_exam_anomaly_features
from projects.daotao_ai.gold.domain.video_anomaly.aggregator import build_video_anomaly_features
from projects.daotao_ai.gold.pipelines.runtime import (
    load_batch_inputs,
    resolve_output_path,
    run_periodic_job,
    write_batch_output,
)
from projects.daotao_ai.gold.serving_registry import GoldServingRegistry

STREAMING_OUTPUTS = {
    "video_friction_signals": lambda inputs, config: build_video_anomaly_features(
        inputs["video"], config.bucket_seconds
    ),
    "exam_integrity_signals": lambda inputs, config: build_exam_anomaly_features(
        inputs["exam_attempts"], inputs["system"]
    ),
    "behavior_anomaly_signals": lambda inputs, config: build_behavior_anomalies(
        inputs["video"], inputs["pdf"], inputs["performance"], inputs["learning"]
    ),
}

def run(config: GoldConfig) -> None:
    spark = build_spark(f"{config.app_name}_stream")
    registry = GoldServingRegistry.from_env()
    streaming_specs = registry.outputs_by_mode("streaming")
    interval_seconds = int(config.trigger_interval_seconds)

    def cycle() -> None:
        inputs = load_batch_inputs(spark, config, registry.required_input_keys("streaming"))
        for spec in streaming_specs:
            output_df = STREAMING_OUTPUTS[spec.name](inputs, config)
            write_batch_output(
                output_df,
                resolve_output_path(config, spec.name),
                spec.partition_by,
            )

    run_periodic_job(f"{config.app_name}_stream", interval_seconds, cycle)
