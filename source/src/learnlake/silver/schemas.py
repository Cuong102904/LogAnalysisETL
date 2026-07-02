from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RouteLeafMatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    op: Literal["eq", "in", "regex", "contains", "startswith", "exists"]
    value: str | int | float | bool | None = None
    values: list[str | int | float | bool] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_leaf(self) -> "RouteLeafMatch":
        if self.op == "in" and not self.values:
            raise ValueError("route match operator 'in' requires non-empty values")
        if self.op != "in" and self.op != "exists" and self.value is None:
            raise ValueError(f"route match operator '{self.op}' requires value")
        return self


class RouteMatchNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    all: list["RouteMatchNode"] | None = None
    any: list["RouteMatchNode"] | None = None
    not_: "RouteMatchNode | None" = Field(default=None, alias="not")
    leaf: RouteLeafMatch | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_input(cls, data):
        if not isinstance(data, dict):
            raise ValueError("route match node must be an object")
        if any(key in data for key in ("all", "any", "not")):
            return data
        return {"leaf": data}

    @model_validator(mode="after")
    def validate_single_node(self) -> "RouteMatchNode":
        present = sum(
            node is not None
            for node in (
                self.all,
                self.any,
                self.not_,
                self.leaf,
            )
        )
        if present != 1:
            raise ValueError("route match node must define exactly one operation")
        return self


RouteMatchNode.model_rebuild()


class RouteClassification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_group: str
    event_subgroup: str
    parser_family: str
    is_noise: bool = False


class RouteTargets(BaseModel):
    model_config = ConfigDict(extra="forbid")

    canonical: bool = True
    domain: list[str] = Field(default_factory=list)


class SilverRoute(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    priority: int = 100
    description: str | None = None
    family: str
    match: RouteMatchNode
    classify: RouteClassification
    targets: RouteTargets
    parser: str


class SilverRoutingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    defaults: dict[str, str | int | bool] = Field(default_factory=dict)
    routes: list[SilverRoute]

    @model_validator(mode="after")
    def validate_unique_ids(self) -> "SilverRoutingConfig":
        seen: set[str] = set()
        for route in self.routes:
            if route.id in seen:
                raise ValueError(f"duplicate route id: {route.id}")
            seen.add(route.id)
        return self


class ParserInputs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required: list[str] = Field(default_factory=list)
    optional: list[str] = Field(default_factory=list)


class ParserOutputs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    domain_tables: list[str] = Field(default_factory=list)
    canonical_fields: dict[str, str | bool | int | float] = Field(default_factory=dict)


class ParserDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    parser_family: str
    description: str
    inputs: ParserInputs
    outputs: ParserOutputs


class SilverParserConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    defaults: dict[str, str | int | bool] = Field(default_factory=dict)
    parsers: list[ParserDefinition]

    @model_validator(mode="after")
    def validate_unique_names(self) -> "SilverParserConfig":
        seen: set[str] = set()
        for parser in self.parsers:
            if parser.name in seen:
                raise ValueError(f"duplicate parser name: {parser.name}")
            seen.add(parser.name)
        return self
