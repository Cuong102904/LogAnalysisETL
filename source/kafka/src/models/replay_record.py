from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ReplayRecord:
    raw_line: str
    event: dict[str, Any] | None
    key_bytes: bytes | None
    event_time: datetime | None
    decode_error: str | None
    validation_ok: bool
    validation_reason: str
