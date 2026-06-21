from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs


@dataclass(frozen=True)
class ParsedPayload:
    kind: str
    data: Any

    def json_dict(self) -> dict[str, Any] | None:
        return self.data if isinstance(self.data, dict) else None


def parse_event_payload(event: Any) -> ParsedPayload:
    if isinstance(event, dict):
        return ParsedPayload(kind="json_dict", data=event)
    if isinstance(event, list):
        return ParsedPayload(kind="list", data=event)
    if isinstance(event, str):
        text = event.strip()
        if not text:
            return ParsedPayload(kind="empty_string", data=None)
        try:
            return ParsedPayload(kind="json_string", data=json.loads(text))
        except json.JSONDecodeError:
            if "=" in text or "&" in text:
                return ParsedPayload(kind="form_encoded", data=parse_qs(text, keep_blank_values=True))
            return ParsedPayload(kind="invalid_string", data=text)
    if event is None:
        return ParsedPayload(kind="null", data=None)
    return ParsedPayload(kind="unknown", data=event)
