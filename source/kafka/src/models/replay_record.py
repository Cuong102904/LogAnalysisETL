from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class ReplayRecord:
    raw_line: str
    event: Optional[dict[str, Any]]
    key_bytes: Optional[bytes]
    event_time: Optional[datetime]
    decode_error: Optional[str]
    validation_ok: bool
    validation_reason: str
