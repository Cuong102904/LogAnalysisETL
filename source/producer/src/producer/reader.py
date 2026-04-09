from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class LogEvent:
    ts: datetime
    raw_line: str
    record: dict[str, Any] | None
    source_path: Path
    line_no: int


def iter_log_files(data_dir: Path) -> list[Path]:
    return sorted(p for p in data_dir.rglob("tracking.log-*") if p.is_file())


def parse_event_time(record: dict[str, Any]) -> datetime | None:
    val = record.get("time")
    if not isinstance(val, str) or not val.strip():
        return None
    s = val.strip()
    try:
        # Handles "2025-10-01T04:37:24.844309+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def iter_events_from_file(path: Path, max_events: int = 0) -> Iterable[LogEvent]:
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for idx, line in enumerate(fh, start=1):
            if max_events and idx > max_events:
                break
            raw = line.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                yield LogEvent(
                    ts=datetime.fromtimestamp(0, tz=timezone.utc),
                    raw_line=raw,
                    record=None,
                    source_path=path,
                    line_no=idx,
                )
                continue
            if not isinstance(rec, dict):
                yield LogEvent(
                    ts=datetime.fromtimestamp(0, tz=timezone.utc),
                    raw_line=raw,
                    record=None,
                    source_path=path,
                    line_no=idx,
                )
                continue
            ts = parse_event_time(rec) or datetime.fromtimestamp(0, tz=timezone.utc)
            yield LogEvent(ts=ts, raw_line=raw, record=rec, source_path=path, line_no=idx)


def load_and_sort_events(
    data_dir: Path,
    max_events_total: int = 0,
) -> list[LogEvent]:
    events: list[LogEvent] = []
    for path in iter_log_files(data_dir):
        for ev in iter_events_from_file(path):
            events.append(ev)
            if max_events_total and len(events) >= max_events_total:
                break
        if max_events_total and len(events) >= max_events_total:
            break
    events.sort(key=lambda e: e.ts)
    return events

