from __future__ import annotations

import json
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from learnlake.normalization.values import get_path, parse_timestamp
from learnlake.runtime.config import resolve_path

TRACKING_FILE_RE = re.compile(r"^tracking\.log-(\d{8})-(\d+)(?:\.json.*)?$")
TRACKING_FILE_GLOB = "tracking.log-*"
DEFAULT_READ_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class TrackingReplayRecord:
    raw_line: str
    event: dict[str, Any] | None
    event_time: datetime | None
    decode_error: str | None
    source_path: Path


def _tracking_file_sort_key(path: Path) -> tuple[int, str, int, str]:
    match = TRACKING_FILE_RE.match(path.name)
    if match is None:
        return (1, str(path.parent), -1, path.name)
    day_token, epoch_token = match.groups()
    return (0, day_token, int(epoch_token), str(path))


def _iter_tracking_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    files = [path for path in root.rglob(TRACKING_FILE_GLOB) if path.is_file()]
    return sorted(files, key=_tracking_file_sort_key)


def _extract_event_time(event: dict[str, Any], event_time_field: str) -> datetime | None:
    return parse_timestamp(get_path(event, event_time_field))


def _drain_payload_buffer(
    buffer: str,
    *,
    decoder: json.JSONDecoder,
    final: bool,
) -> tuple[str, list[tuple[str, dict[str, Any] | None, str | None]]]:
    emitted: list[tuple[str, dict[str, Any] | None, str | None]] = []
    while True:
        stripped = buffer.lstrip()
        if not stripped:
            return "", emitted

        try:
            event, end_idx = decoder.raw_decode(stripped)
        except json.JSONDecodeError as err:
            if not final:
                return stripped, emitted

            raw_payload = stripped.strip()
            if raw_payload:
                emitted.append((raw_payload, None, str(err)))
            return "", emitted

        raw_payload = stripped[:end_idx].strip()
        if isinstance(event, dict):
            emitted.append((raw_payload, event, None))
        else:
            emitted.append((raw_payload, None, "event payload must be a JSON object"))
        buffer = stripped[end_idx:]


def _iter_jsonl_payloads(file_path: Path) -> Iterator[tuple[str, dict[str, Any] | None, str | None]]:
    with file_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            raw_payload = line.strip()
            if not raw_payload:
                continue
            try:
                event = json.loads(raw_payload)
            except json.JSONDecodeError as err:
                yield raw_payload, None, str(err)
                continue

            if isinstance(event, dict):
                yield raw_payload, event, None
            else:
                yield raw_payload, None, "event payload must be a JSON object"


def _should_parse_as_jsonl(file_path: Path) -> bool:
    with file_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            raw_payload = line.strip()
            if not raw_payload:
                continue
            try:
                event = json.loads(raw_payload)
            except json.JSONDecodeError:
                return False
            return isinstance(event, dict)
    return True


def _iter_payloads_from_file(
    file_path: Path,
    *,
    chunk_size: int = DEFAULT_READ_CHUNK_SIZE,
) -> Iterator[tuple[str, dict[str, Any] | None, str | None]]:
    if _should_parse_as_jsonl(file_path):
        yield from _iter_jsonl_payloads(file_path)
        return

    decoder = json.JSONDecoder()
    buffer = ""

    with file_path.open("r", encoding="utf-8", errors="replace") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            buffer, emitted = _drain_payload_buffer(buffer, decoder=decoder, final=False)
            yield from emitted

    buffer, emitted = _drain_payload_buffer(buffer, decoder=decoder, final=True)
    yield from emitted


def iter_tracking_log_records(
    input_root: str | Path,
    *,
    event_time_field: str,
    max_files: int = 0,
    max_lines: int = 0,
    skip_decode_errors: bool = False,
) -> Iterator[TrackingReplayRecord]:
    root = resolve_path(input_root)
    if not root.exists():
        raise FileNotFoundError(f"tracking log root does not exist: {root}")

    files = _iter_tracking_files(root)
    if max_files > 0:
        files = files[:max_files]

    produced_valid_records = 0

    for file_path in files:
        for raw_payload, event, decode_error in _iter_payloads_from_file(file_path):
            if skip_decode_errors and decode_error is not None:
                continue

            event_time = None
            if event is not None:
                event_time = _extract_event_time(event, event_time_field)

            if event is not None and decode_error is None:
                produced_valid_records += 1
                if max_lines > 0 and produced_valid_records > max_lines:
                    return

            yield TrackingReplayRecord(
                raw_line=raw_payload,
                event=event,
                event_time=event_time,
                decode_error=decode_error,
                source_path=file_path,
            )
