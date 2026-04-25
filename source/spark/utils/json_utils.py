import json
from typing import Any


def parse_json_or_none(raw: str) -> dict[str, Any] | None:
    try:
        value = json.loads(raw)
    except Exception:
        return None
    return value if isinstance(value, dict) else None
