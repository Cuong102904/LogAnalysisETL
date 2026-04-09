from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProducerConfig:
    brokers: str
    data_dir: Path
    raw_topic: str
    canonical_topic: str
    dlq_topic: str
    speed: float
    max_events: int
    key_mode: str
    since: str | None
    until: str | None

