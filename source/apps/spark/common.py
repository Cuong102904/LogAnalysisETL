from __future__ import annotations

import os
from importlib import import_module
from pathlib import Path
from typing import Any

from learnlake.connectors import read_json_lines, write_json_lines
from learnlake.contracts import MappingSpec, RouteSet
from learnlake.normalization import EventTypeResolver, MappingEvaluator
from learnlake.plugins import TransformRegistry
from learnlake.quality import load_rule_set
from learnlake.runtime import (
    load_mapping_spec,
    load_metric_definition,
    load_route_set,
    load_source_profile,
)
from learnlake.runtime.config import resolve_path


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

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if bootstrap_servers:
        input_updates["mode"] = "kafka"
        input_updates["bootstrap_servers"] = bootstrap_servers
        input_updates["starting_offsets"] = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
        max_offsets_per_trigger = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER")
        if max_offsets_per_trigger:
            input_updates["max_offsets_per_trigger"] = int(max_offsets_per_trigger)
        trigger_processing_time = os.getenv("KAFKA_TRIGGER_PROCESSING_TIME")
        if trigger_processing_time:
            input_updates["trigger_processing_time"] = trigger_processing_time

    if input_updates:
        profile = profile.model_copy(update={"input": profile.input.model_copy(update=input_updates)})
    return profile


def _load_source_registry(source_id: str) -> TransformRegistry:
    registry = TransformRegistry()
    try:
        module = import_module(f"projects.{source_id}.transforms")
    except ModuleNotFoundError:
        return registry
    register = getattr(module, "register_transforms", None)
    if callable(register):
        return register(registry)
    return registry


def build_mapping_evaluator(mapping: MappingSpec, source_id: str | None = None) -> MappingEvaluator:
    resolvers = {}
    if mapping.event_type_map:
        resolver_name = Path(mapping.event_type_map).stem
        resolvers[resolver_name] = EventTypeResolver.from_yaml(resolve_path(mapping.event_type_map))
    registry = _load_source_registry(source_id or mapping.source_id)
    return MappingEvaluator(mapping, resolvers=resolvers, registry=registry)


def load_source_mapping(source_id: str) -> tuple[Any, MappingSpec, RouteSet, MappingEvaluator]:
    profile = load_profile(source_id)
    mapping = load_mapping_spec(resolve_path(profile.silver.mapping or ""))
    routes = load_route_set(resolve_path(profile.silver.routing))
    evaluator = build_mapping_evaluator(mapping, source_id)
    return profile, mapping, routes, evaluator


def load_source_quality_rules(source_id: str):
    profile = load_profile(source_id)
    rules = []
    for rule_path in profile.silver.quality_rules:
        rules.extend(load_rule_set(resolve_path(rule_path)).rules)
    return rules


def load_source_metric(source_id: str, metric_id: str):
    profile = load_profile(source_id)
    for metric_path in profile.metrics:
        definition = load_metric_definition(resolve_path(metric_path))
        if definition.metric_id == metric_id:
            return definition
    raise ValueError(f"Metric {metric_id} is not declared by source {source_id}")
