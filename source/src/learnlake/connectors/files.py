from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any


def read_json_lines(path: str | Path) -> Iterator[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                value = json.loads(stripped)
                if not isinstance(value, dict):
                    raise ValueError("JSON line must decode to object")
                yield value


def write_json_lines(path: str | Path, records: list[dict[str, Any]]) -> None:
    resolved = Path(path)
    if resolved.suffix != ".jsonl":
        resolved.mkdir(parents=True, exist_ok=True)
        resolved = resolved / "part-00000.jsonl"
    else:
        resolved.parent.mkdir(parents=True, exist_ok=True)
    with resolved.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, default=str, sort_keys=True))
            f.write("\n")
