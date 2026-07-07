from __future__ import annotations

import os
from typing import Any

from learnlake.runtime import load_source_profile
from learnlake.silver.runtime import SilverPlan, load_silver_plan


def _int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    return int(value)


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


def load_source_silver_plan(source_id: str) -> SilverPlan:
    plan = load_silver_plan(source_id)
    runtime_updates: dict[str, Any] = {}

    trigger_processing_time = os.getenv("SILVER_TRIGGER_PROCESSING_TIME")
    if trigger_processing_time:
        runtime_updates["trigger_processing_time"] = trigger_processing_time

    max_files_per_trigger = _int_env("SILVER_MAX_FILES_PER_TRIGGER")
    if max_files_per_trigger is not None:
        runtime_updates["max_files_per_trigger"] = max_files_per_trigger

    if runtime_updates:
        plan = SilverPlan(
            profile=plan.profile.model_copy(
                update={
                    "silver": plan.profile.silver.model_copy(
                        update={"runtime": plan.profile.silver.runtime.model_copy(update=runtime_updates)}
                    )
                }
            ),
            routing=plan.routing,
            parsers=plan.parsers,
            compiled_routes=plan.compiled_routes,
            compiled_quality_rules=plan.compiled_quality_rules,
        )
    return plan
