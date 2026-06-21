from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def get_path(record: dict[str, Any] | None, path: str | None) -> Any:
    if record is None or not path:
        return None
    current: Any = record
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def slugify_identifier(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, int | float):
        number = float(value)
        if number > 10_000_000_000:
            number = number / 1000
        return datetime.fromtimestamp(number, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def cast_value(value: Any, target_type: str | None) -> Any:
    if target_type is None or value is None:
        return value
    if target_type == "string":
        return str(value)
    if target_type in {"int", "long"}:
        return int(value)
    if target_type == "float":
        return float(value)
    if target_type == "boolean":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y"}
    if target_type == "timestamp":
        return parse_timestamp(value)
    if target_type == "date":
        parsed = parse_timestamp(value)
        return None if parsed is None else parsed.date()
    if target_type == "json":
        if isinstance(value, str):
            return json.loads(value)
        return value
    raise ValueError(f"Unsupported cast target {target_type}")
