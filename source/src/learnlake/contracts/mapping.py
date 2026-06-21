from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

MappingCast = Literal["string", "int", "long", "float", "boolean", "timestamp", "date", "json"]


class ResolverSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    input: str
    output: str


class PluginSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    inputs: list[dict[str, Any]] = Field(default_factory=list)


class FieldMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str | None = None
    const: Any = None
    coalesce: list[FieldMapping] | None = None
    cast: MappingCast | None = None
    resolver: ResolverSpec | None = None
    plugin: PluginSpec | None = None

    @model_validator(mode="after")
    def validate_single_operation(self) -> FieldMapping:
        operations = [
            self.path is not None,
            self.const is not None,
            self.coalesce is not None,
            self.resolver is not None,
            self.plugin is not None,
        ]
        if sum(operations) != 1:
            raise ValueError("field mapping must define exactly one operation")
        return self


class MappingSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: str
    target: str
    event_type_map: str | None = None
    fields: dict[str, FieldMapping]
