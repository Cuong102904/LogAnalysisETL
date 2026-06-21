from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class InputConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["kafka", "file", "fixture"]
    format: str = "json"
    topic: str | None = None
    path: str | None = None
    bootstrap_servers: str | None = None
    starting_offsets: str = "latest"
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
    mapping: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    partition_by: list[str] = Field(default_factory=list)
    quality_rules: list[str] = Field(default_factory=list)


class InvalidTargetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    table: str
    path: str
    checkpoint: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")


class SilverConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_index: SilverTargetConfig
    targets: dict[str, SilverTargetConfig] = Field(default_factory=dict)
    invalid: InvalidTargetConfig | None = None
    routing: str
    quality_rules: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_targets(self) -> "SilverConfig":
        if not self.event_index.table or not self.event_index.path:
            raise ValueError("silver.event_index requires non-empty table and path")
        if not self.event_index.mapping:
            raise ValueError("silver.event_index requires a mapping reference")
        if not self.routing:
            raise ValueError("silver.routing is required")
        for target_name, target in self.targets.items():
            if not target.table or not target.path:
                raise ValueError(
                    f"silver.targets.{target_name} requires non-empty table and path"
                )
        if self.invalid is not None and (not self.invalid.table or not self.invalid.path):
            raise ValueError("silver.invalid requires non-empty table and path")
        return self

    @property
    def table(self) -> str:
        return self.event_index.table

    @property
    def path(self) -> str:
        return self.event_index.path

    @property
    def checkpoint(self) -> str | None:
        return self.event_index.checkpoint

    @property
    def mapping(self) -> str | None:
        return self.event_index.mapping

    @property
    def invalid_path(self) -> str | None:
        return None if self.invalid is None else self.invalid.path

    def target_for_table(self, table_name: str) -> SilverTargetConfig | None:
        if self.event_index.table == table_name:
            return self.event_index
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
