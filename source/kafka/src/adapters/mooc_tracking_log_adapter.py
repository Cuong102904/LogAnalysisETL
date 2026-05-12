from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from kafka.src.common import validate_tracking_event
from kafka.src.models.replay_record import ReplayRecord
from kafka.src.producers.replayer.pacing import parse_event_time


def _iter_tracking_files(root: Path) -> list[Path]:
    return [p for p in root.rglob("tracking.log-*.json*") if p.is_file()]


def _extract_key(event: dict[str, Any]) -> bytes:
    username = str(event.get("username") or "").strip()
    session = str(event.get("session") or "").strip()
    ip = str(event.get("ip") or "").strip()
    if username:
        return f"user:{username}".encode()
    if session:
        return f"session:{session}".encode()
    return f"ip:{ip or 'unknown'}".encode()


def _iter_payloads(text: str) -> Iterable[tuple[str, dict[str, Any] | None, str | None]]:
    """
    Yield (raw_payload, event, decode_error) for each JSON value in the file.

    The tracking logs are stored as pretty-printed JSON objects, sometimes with
    many lines per object. Using raw_decode avoids brace-count parsing bugs.
    """

    decoder = json.JSONDecoder()
    idx = 0
    text_len = len(text)

    while idx < text_len:
        while idx < text_len and text[idx].isspace():
            idx += 1
        if idx >= text_len:
            break

        try:
            event, end_idx = decoder.raw_decode(text, idx)
            raw_payload = text[idx:end_idx].strip()
            decode_error = None
        except json.JSONDecodeError as err:
            line_end = text.find("\n", idx)
            if line_end == -1:
                line_end = text_len
            raw_payload = text[idx:line_end].strip()
            event = None
            decode_error = str(err)
            idx = line_end + 1 if line_end < text_len else text_len
            yield raw_payload, event, decode_error
            continue

        idx = end_idx

        if not isinstance(event, dict):
            yield raw_payload, None, "event payload must be a JSON object"
            continue

        yield raw_payload, event, None


def iter_mooc_tracking_log_records(
    *,
    input_root: Path,
    max_files: int = 0,
    max_lines: int = 0,
) -> Iterable[ReplayRecord]:
    """
    Adapter responsibility:
    - discover tracking.log-*.json* files
    - read and parse each JSON object (supports JSON lines and pretty-printed)
    - perform minimal validation (required fields)

    It does not do Kafka routing or DLQ publishing.
    """

    root = input_root.resolve()
    files = _iter_tracking_files(root)
    if max_files > 0:
        files = files[:max_files]

    produced_valid_records = 0

    for file_path in files:
        with file_path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw_payload, event, decode_error in _iter_payloads(handle.read()):
                event_time = None
                key_bytes = None
                validation_ok = False
                validation_reason = ""

                if decode_error is not None:
                    validation_ok = False
                    validation_reason = "decode_json_failed"
                elif event is not None:
                    event_time = parse_event_time(event.get("time"))
                    validation_ok, validation_reason = validate_tracking_event(event)
                    if validation_ok:
                        key_bytes = _extract_key(event)

                # Respect max_lines semantics: count only structurally valid events.
                if validation_ok:
                    produced_valid_records += 1
                    if max_lines > 0 and produced_valid_records > max_lines:
                        return

                yield ReplayRecord(
                    raw_line=raw_payload,
                    event=event,
                    key_bytes=key_bytes,
                    event_time=event_time,
                    decode_error=decode_error,
                    validation_ok=validation_ok,
                    validation_reason=validation_reason,
                )
