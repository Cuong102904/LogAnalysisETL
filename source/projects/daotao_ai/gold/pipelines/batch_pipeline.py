from __future__ import annotations

from learnlake.runtime import build_spark
from projects.daotao_ai.gold.aggregation_config import GoldConfig
from projects.daotao_ai.gold.domain.learning_journey.aggregator import (
    build_learning_journey_features,
)
from projects.daotao_ai.gold.domain.pdf_behavior.aggregator import build_pdf_behavior_features
from projects.daotao_ai.gold.domain.quiz_performance.aggregator import (
    build_quiz_performance_features,
)
from projects.daotao_ai.gold.pipelines.runtime import (
    load_batch_inputs,
    resolve_output_path,
    write_batch_output,
)
from projects.daotao_ai.gold.serving_registry import GoldServingRegistry


def _build_batch_outputs(inputs: dict[str, object]) -> dict[str, object]:
    return {
        "pdf_engagement_features": build_pdf_behavior_features(inputs["pdf"]),
        "quiz_attempt_metrics": build_quiz_performance_features(inputs["performance"]),
        "user_learning_profile_daily": build_learning_journey_features(inputs["learning"]),
    }


def run(config: GoldConfig) -> None:
    spark = build_spark(f"{config.app_name}_batch")
    registry = GoldServingRegistry.from_env()
    batch_specs = registry.outputs_by_mode("batch")
    input_keys = registry.required_input_keys("batch")
    inputs = load_batch_inputs(spark, config, input_keys)
    outputs = _build_batch_outputs(inputs)

    for spec in batch_specs:
        df = outputs[spec.name]
        write_batch_output(df, resolve_output_path(config, spec.name), spec.partition_by)
