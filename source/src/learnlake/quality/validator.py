from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from learnlake.contracts import QualityRule


def _get_path(record: dict[str, Any] | None, path: str | None) -> Any:
    if record is None or not path:
        return None
    current: Any = record
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


@dataclass(frozen=True)
class ValidationResult:
    status: str = "valid"
    errors: list[str] = field(default_factory=list)


def _worse_status(current: str, candidate: str) -> str:
    order = {"valid": 0, "warning": 1, "ignored": 2, "invalid": 3}
    return candidate if order[candidate] > order[current] else current


def validate_record(
    record: dict[str, Any],
    rules: list[QualityRule],
) -> ValidationResult:
    status = str(record.get("quality_status") or "valid")
    errors = list(record.get("quality_errors") or [])

    for rule in rules:
        failed = False
        if rule.type == "required":
            failed = _get_path(record, rule.field) in {None, ""}
        elif rule.type == "allowed_values":
            failed = _get_path(record, rule.field) not in set(rule.allowed)
        elif rule.type == "boolean_flag":
            failed = bool(_get_path(record, rule.field)) is True
        elif rule.type == "mark_when_equals":
            failed = _get_path(record, rule.field) == rule.value
        elif rule.type == "unique":
            failed = False

        if failed:
            status = _worse_status(status, rule.status)
            errors.append(rule.message or rule.id)

    return ValidationResult(status=status, errors=errors)


def validate_learning_event_record(
    record: dict[str, Any],
    rules: list[QualityRule],
) -> ValidationResult:
    return validate_record(record, rules)
