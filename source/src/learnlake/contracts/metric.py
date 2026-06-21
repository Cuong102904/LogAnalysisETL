from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class MetricExecutionProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["batch", "streaming"]
    priority: Literal["low", "normal", "high"] = "normal"
    trigger_interval: str | None = None
    enabled: bool = True
    resources: dict[str, str | int | float] = Field(default_factory=dict)


class MetricDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric_id: str
    input: str
    output: str
    window: str = "1 hour"
    execution: MetricExecutionProfile
