from __future__ import annotations

from pathlib import Path
from typing import Any

from learnlake.connectors import write_json_lines


def write_bronze_records(path: str | Path, records: list[dict[str, Any]]) -> None:
    for record in records:
        if not record.get("source_id") or not record.get("processing_date"):
            raise ValueError("Bronze records must include source_id and processing_date")
    write_json_lines(path, records)
