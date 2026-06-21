from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class FactRecord:
    target_table: str
    record: dict[str, Any]
    quality_status: str = "valid"
    quality_errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class NormalizationResult:
    event_index: dict[str, Any] | None = None
    facts: list[FactRecord] = field(default_factory=list)
    invalid_records: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class BatchNormalizationResult:
    records_by_target: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    invalid_records: list[dict[str, Any]] = field(default_factory=list)

