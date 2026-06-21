from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

QualitySeverity = Literal["warning", "invalid", "ignored"]


class QualityRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: Literal[
        "required",
        "allowed_values",
        "unique",
        "boolean_flag",
        "mark_when_equals",
    ]
    field: str | None = None
    fields: list[str] = Field(default_factory=list)
    allowed: list[Any] = Field(default_factory=list)
    value: Any = None
    status: QualitySeverity = "invalid"
    message: str | None = None


class QualityRuleSet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rules: list[QualityRule]
