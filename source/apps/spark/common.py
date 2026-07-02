from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from learnlake.connectors import read_json_lines, write_json_lines
from learnlake.runtime import load_metric_definition, load_source_profile
from learnlake.runtime.config import resolve_path
from learnlake.silver.runtime import SilverPlan, load_silver_plan


def read_records(path: str | Path) -> list[dict[str, Any]]:
    resolved = resolve_path(path)
    if resolved.is_dir():
        records: list[dict[str, Any]] = []
        for file_path in sorted(resolved.glob("*.jsonl")):
            records.extend(read_json_lines(file_path))
        return records
    return list(read_json_lines(resolved))


def write_records(path: str | Path, records: list[dict[str, Any]]) -> None:
    write_json_lines(resolve_path(path), records)


def load_profile(source_id: str):
    profile = load_source_profile(source_id)
    input_updates: dict[str, Any] = {}

    bootstrap_servers = os.getenv("BRONZE_BOOTSTRAP_SERVERS")
    if bootstrap_servers:
        input_updates["mode"] = "kafka"
        input_updates["bootstrap_servers"] = bootstrap_servers
        input_updates["starting_offsets"] = os.getenv("BRONZE_STARTING_OFFSETS", "earliest")
        max_offsets_per_trigger = os.getenv("BRONZE_MAX_OFFSETS_PER_TRIGGER")
        if max_offsets_per_trigger:
            input_updates["max_offsets_per_trigger"] = int(max_offsets_per_trigger)
        trigger_processing_time = os.getenv("BRONZE_TRIGGER_PROCESSING_TIME")
        if trigger_processing_time:
            input_updates["trigger_processing_time"] = trigger_processing_time

    if input_updates:
        profile = profile.model_copy(update={"input": profile.input.model_copy(update=input_updates)})
    return profile


def load_source_metric(source_id: str, metric_id: str):
    profile = load_profile(source_id)
    for metric_path in profile.metrics:
        definition = load_metric_definition(resolve_path(metric_path))
        if definition.metric_id == metric_id:
            return definition
    raise ValueError(f"Metric {metric_id} is not declared by source {source_id}")


def load_source_silver_plan(source_id: str) -> SilverPlan:
    return load_silver_plan(source_id)
