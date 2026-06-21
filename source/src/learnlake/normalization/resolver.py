from __future__ import annotations

from pathlib import Path
from typing import Any

from learnlake.runtime.config import load_yaml


class EventTypeResolver:
    def __init__(self, mapping: dict[str, Any]) -> None:
        self._mapping = mapping.get("mappings", {})
        self._unknown = mapping.get(
            "unknown",
            {
                "action": "unknown",
                "object_type": "unknown",
                "event_category": "unknown",
                "learning_relevance": "unknown",
            },
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> EventTypeResolver:
        return cls(load_yaml(path))

    def resolve(self, event_type: Any, output: str) -> Any:
        if event_type is None:
            return self._unknown.get(output)
        entry = self._mapping.get(str(event_type), self._unknown)
        return entry.get(output, self._unknown.get(output))
