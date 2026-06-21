from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RouteMatch(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    field: str | None = None
    equals: Any = None
    in_values: list[Any] | None = Field(default=None, alias="in")
    starts_with: str | None = None
    contains: str | None = None
    regex: str | None = None
    has_key: str | None = None
    all_of: list["RouteMatch"] | None = Field(default=None, alias="all")
    any_of: list["RouteMatch"] | None = Field(default=None, alias="any")
    not_match: "RouteMatch | None" = Field(default=None, alias="not")

    @model_validator(mode="after")
    def validate_single_operation(self) -> "RouteMatch":
        operations = [
            self.equals is not None,
            self.in_values is not None,
            self.starts_with is not None,
            self.contains is not None,
            self.regex is not None,
            self.has_key is not None,
            self.all_of is not None,
            self.any_of is not None,
            self.not_match is not None,
        ]
        if sum(operations) != 1:
            raise ValueError("route match must define exactly one operation")
        if self.all_of is None and self.any_of is None and self.not_match is None and not self.field:
            raise ValueError("field is required for leaf route matches")
        return self


RouteMatch.model_rebuild()


class RouteSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    priority: int = 100
    match: RouteMatch
    event_group: str
    normalized_type: str
    action: str
    object_type: str
    learning_relevance: Literal["learning", "non_learning", "noise", "unknown"] = "unknown"
    is_noise: bool = False
    targets: list[str]
    extractor: str | None = None

    @model_validator(mode="after")
    def validate_targets_and_extractor(self) -> "RouteSpec":
        if not self.targets or any(not target for target in self.targets):
            raise ValueError("route targets must contain at least one non-empty target table")
        if self.extractor is not None and not self.extractor.strip():
            raise ValueError("route extractor must be a non-empty string when provided")
        return self


class RouteSet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    routes: list[RouteSpec]

    @model_validator(mode="after")
    def validate_unique_route_ids(self) -> "RouteSet":
        seen: set[str] = set()
        for route in self.routes:
            if route.id in seen:
                raise ValueError(f"duplicate route id: {route.id}")
            seen.add(route.id)
        return self
