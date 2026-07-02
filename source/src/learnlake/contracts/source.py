from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Literal


class InputConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["kafka", "file", "fixture"]
    format: str = "json"
    topic: str | None = None
    path: str | None = None
    bootstrap_servers: str | None = None
    starting_offsets: str = "latest"
    max_offsets_per_trigger: int | None = None
    trigger_processing_time: str | None = None
    event_time_field: str
    event_type_field: str | None = None


class TableConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table: str
    path: str
    checkpoint: str | None = None
    partition_by: list[str] = Field(default_factory=list)


class SilverTargetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    table: str
    path: str
    checkpoint: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    partition_by: list[str] = Field(default_factory=list)


class SilverRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    checkpoint: str
    trigger_processing_time: str = "10 seconds"
    max_files_per_trigger: int = 100

    @model_validator(mode="after")
    def validate_runtime(self) -> "SilverRuntimeConfig":
        if not self.checkpoint:
            raise ValueError("silver.runtime.checkpoint is required")
        if not self.trigger_processing_time:
            raise ValueError("silver.runtime.trigger_processing_time is required")
        if self.max_files_per_trigger <= 0:
            raise ValueError("silver.runtime.max_files_per_trigger must be positive")
        return self


class SilverConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runtime: SilverRuntimeConfig
    canonical: SilverTargetConfig
    unknown: SilverTargetConfig
    invalid: SilverTargetConfig
    targets: dict[str, SilverTargetConfig] = Field(default_factory=dict)
    routing: str
    parsers: str
    quality_rules: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_targets(self) -> "SilverConfig":
        if not self.canonical.table or not self.canonical.path:
            raise ValueError("silver.canonical requires non-empty table and path")
        if not self.unknown.table or not self.unknown.path:
            raise ValueError("silver.unknown requires non-empty table and path")
        if not self.invalid.table or not self.invalid.path:
            raise ValueError("silver.invalid requires non-empty table and path")
        if not self.routing:
            raise ValueError("silver.routing is required")
        if not self.parsers:
            raise ValueError("silver.parsers is required")
        for target_name, target in self.targets.items():
            if not target.table or not target.path:
                raise ValueError(
                    f"silver.targets.{target_name} requires non-empty table and path"
                )
        return self

    @property
    def table(self) -> str:
        return self.canonical.table

    @property
    def path(self) -> str:
        return self.canonical.path

    @property
    def checkpoint(self) -> str:
        return self.runtime.checkpoint

    @property
    def invalid_path(self) -> str:
        return self.invalid.path

    def target_for_table(self, table_name: str) -> SilverTargetConfig | None:
        if self.canonical.table == table_name:
            return self.canonical
        if self.unknown.table == table_name:
            return self.unknown
        if self.invalid.table == table_name:
            return self.invalid
        for target in self.targets.values():
            if target.table == table_name:
                return target
        return None


class SourceProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: str
    source_type: str
    domain: str | None = None
    input: InputConfig
    bronze: TableConfig
    silver: SilverConfig
    metrics: list[str] = Field(default_factory=list)
    workflows: list[str] = Field(default_factory=list)
