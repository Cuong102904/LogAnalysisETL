from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from learnlake.runtime.config import load_yaml

Mode = Literal["streaming", "batch"]

_INPUT_PATH_ATTRS: dict[str, str] = {
    "learning": "input_learning_path",
    "video": "input_video_path",
    "pdf": "input_pdf_path",
    "performance": "input_performance_path",
    "exam_attempts": "input_exam_attempts_path",
    "system": "input_system_path",
}

_OUTPUT_PATH_ATTRS: dict[str, str] = {
    "video_friction_signals": "output_video_friction_signals_path",
    "pdf_engagement_features": "output_pdf_engagement_features_path",
    "quiz_attempt_metrics": "output_quiz_attempt_metrics_path",
    "user_learning_profile_daily": "output_user_learning_profile_daily_path",
    "exam_integrity_signals": "output_exam_integrity_signals_path",
    "behavior_anomaly_signals": "output_behavior_anomaly_signals_path",
}


@dataclass(frozen=True)
class GoldOutputCadence:
    name: str
    mode: Mode
    input_keys: tuple[str, ...]
    partition_by: tuple[str, ...]
    trigger_policy: dict[str, Any]


@dataclass(frozen=True)
class GoldServingRegistry:
    app_name: str
    outputs: tuple[GoldOutputCadence, ...]

    @classmethod
    def from_env(cls) -> GoldServingRegistry:
        return cls.from_path(_registry_path_from_env())

    @classmethod
    def from_path(cls, path: str | Path) -> GoldServingRegistry:
        payload = load_yaml(path)
        outputs: list[GoldOutputCadence] = []
        for raw in payload.get("outputs", []):
            mode = raw.get("mode")
            if mode not in ("streaming", "batch"):
                raise ValueError(f"Unsupported gold mode for {raw.get('name')}: {mode}")
            outputs.append(
                GoldOutputCadence(
                    name=str(raw["name"]),
                    mode=mode,
                    input_keys=tuple(str(item) for item in raw.get("input_keys", [])),
                    partition_by=tuple(str(item) for item in raw.get("partition_by", [])),
                    trigger_policy=dict(raw.get("trigger_policy", {})),
                )
            )
        return cls(app_name=str(payload.get("app_name", "gold_dashboard_serving")), outputs=tuple(outputs))

    def outputs_by_mode(self, mode: Mode) -> tuple[GoldOutputCadence, ...]:
        return tuple(output for output in self.outputs if output.mode == mode)

    def output_names_by_mode(self, mode: Mode) -> tuple[str, ...]:
        return tuple(output.name for output in self.outputs_by_mode(mode))

    def required_input_keys(self, mode: Mode) -> tuple[str, ...]:
        keys: list[str] = []
        for output in self.outputs_by_mode(mode):
            for input_key in output.input_keys:
                if input_key not in keys:
                    keys.append(input_key)
        return tuple(keys)


def _registry_path_from_env() -> str:
    from os import getenv

    return getenv("GOLD_SERVING_REGISTRY_PATH", "catalog/metrics/gold_dashboard_serving.yaml")


def resolve_input_path(config: Any, input_key: str) -> str:
    attr_name = _INPUT_PATH_ATTRS.get(input_key)
    if not attr_name:
        raise KeyError(f"Unknown gold input key: {input_key}")
    return str(getattr(config, attr_name))


def resolve_output_path(config: Any, output_name: str) -> str:
    attr_name = _OUTPUT_PATH_ATTRS.get(output_name)
    if not attr_name:
        raise KeyError(f"Unknown gold output name: {output_name}")
    return str(getattr(config, attr_name))


def load_stream_inputs(
    spark: Any,
    config: Any,
    input_keys: tuple[str, ...],
) -> dict[str, Any]:
    inputs: dict[str, Any] = {}
    for key in input_keys:
        inputs[key] = spark.readStream.format("delta").load(resolve_input_path(config, key))
    return inputs


def load_batch_inputs(
    spark: Any,
    config: Any,
    input_keys: tuple[str, ...],
) -> dict[str, Any]:
    inputs: dict[str, Any] = {}
    for key in input_keys:
        inputs[key] = spark.read.format("delta").load(resolve_input_path(config, key))
    return inputs


def write_stream_output(
    df: Any,
    path: str,
    checkpoint: str,
    query_name: str,
    trigger_seconds: int,
    partition_by: tuple[str, ...],
) -> Any:
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


def write_batch_output(df: Any, path: str, partition_by: tuple[str, ...]) -> None:
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
