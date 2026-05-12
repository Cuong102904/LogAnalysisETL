import json
from typing import Any


def encode_json(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def decode_json(raw: bytes | str | None) -> dict[str, Any]:
    if raw is None:
        raise ValueError("event payload is empty")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("event payload must be a JSON object")
    return payload
