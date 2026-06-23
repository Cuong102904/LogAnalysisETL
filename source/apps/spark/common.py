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
    bronze_updates: dict[str, Any] = {}
    event_index_updates: dict[str, Any] = {}
    invalid_updates: dict[str, Any] = {}
    target_updates: dict[str, dict[str, Any]] = {}

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.getenv("BRONZE_INPUT_TOPIC") or os.getenv("KAFKA_TOPIC_RAW")
    if bootstrap_servers or topic:
        input_updates["mode"] = "kafka"
        if bootstrap_servers:
            input_updates["bootstrap_servers"] = bootstrap_servers
        if topic:
            input_updates["topic"] = topic
        input_updates["starting_offsets"] = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
        max_offsets_per_trigger = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER")
        if max_offsets_per_trigger:
            input_updates["max_offsets_per_trigger"] = int(max_offsets_per_trigger)
        trigger_processing_time = os.getenv("KAFKA_TRIGGER_PROCESSING_TIME")
        if trigger_processing_time:
            input_updates["trigger_processing_time"] = trigger_processing_time

    if os.getenv("BRONZE_TABLE_PATH"):
        bronze_updates["path"] = os.environ["BRONZE_TABLE_PATH"]
    if os.getenv("BRONZE_CHECKPOINT_PATH"):
        bronze_updates["checkpoint"] = os.environ["BRONZE_CHECKPOINT_PATH"]

    if os.getenv("SILVER_EVENT_INDEX_PATH"):
        event_index_updates["path"] = os.environ["SILVER_EVENT_INDEX_PATH"]
    elif os.getenv("SILVER_LEARNING_PATH"):
        event_index_updates["path"] = os.environ["SILVER_LEARNING_PATH"]
    if os.getenv("SILVER_CHECKPOINT_PATH"):
        event_index_updates["checkpoint"] = os.environ["SILVER_CHECKPOINT_PATH"]
    if os.getenv("SILVER_INVALID_PATH"):
        invalid_updates["path"] = os.environ["SILVER_INVALID_PATH"]
    for target_name in profile.silver.targets:
        env_key = f"SILVER_{target_name.upper()}_PATH"
        checkpoint_key = f"SILVER_{target_name.upper()}_CHECKPOINT"
        updates: dict[str, Any] = {}
        if os.getenv(env_key):
            updates["path"] = os.environ[env_key]
        if os.getenv(checkpoint_key):
            updates["checkpoint"] = os.environ[checkpoint_key]
        if updates:
            target_updates[target_name] = updates

    if input_updates:
        profile = profile.model_copy(update={"input": profile.input.model_copy(update=input_updates)})
    if bronze_updates:
        profile = profile.model_copy(update={"bronze": profile.bronze.model_copy(update=bronze_updates)})
    if event_index_updates or invalid_updates or target_updates:
        silver = profile.silver
        update_payload: dict[str, Any] = {}
        if event_index_updates:
            update_payload["event_index"] = silver.event_index.model_copy(update=event_index_updates)
        if target_updates:
            update_payload["targets"] = {
                target_name: silver.targets[target_name].model_copy(update=updates)
                for target_name, updates in target_updates.items()
            } | {
                target_name: target
                for target_name, target in silver.targets.items()
                if target_name not in target_updates
            }
        if invalid_updates and silver.invalid is not None:
            update_payload["invalid"] = silver.invalid.model_copy(update=invalid_updates)
        profile = profile.model_copy(update={"silver": silver.model_copy(update=update_payload)})
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
