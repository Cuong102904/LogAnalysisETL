from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from kafka.src.common import validate_tracking_event
from kafka.src.models.replay_record import ReplayRecord
from kafka.src.producers.replayer.pacing import parse_event_time

TRACKING_FILE_RE = re.compile(r"^tracking\.log-(\d{8})-(\d+)(?:\.json.*)?$")
TRACKING_FILE_GLOB = "tracking.log-*"
DEFAULT_READ_CHUNK_SIZE = 1024 * 1024


def _iter_tracking_files(root: Path) -> list[Path]:
    files = [p for p in root.rglob(TRACKING_FILE_GLOB) if p.is_file()]
    return sorted(files, key=_tracking_file_sort_key)


def _tracking_file_sort_key(path: Path) -> tuple[int, str, int, str]:
    match = TRACKING_FILE_RE.match(path.name)
    if match is None:
        return (1, str(path.parent), -1, path.name)
    day_token, epoch_token = match.groups()
    return (0, day_token, int(epoch_token), str(path))


def _extract_key(event: dict[str, Any]) -> bytes:
    username = str(event.get("username") or "").strip()
    session = str(event.get("session") or "").strip()
    ip = str(event.get("ip") or "").strip()
    if username:
        return f"user:{username}".encode()
    if session:
        return f"session:{session}".encode()
    return f"ip:{ip or 'unknown'}".encode()


def _drain_payload_buffer(
    buffer: str,
    *,
    decoder: json.JSONDecoder,
    final: bool,
) -> Iterable[tuple[str, dict[str, Any] | None, str | None]]:
    while True:
        stripped = buffer.lstrip()
        if not stripped:
            return ""

        try:
            event, end_idx = decoder.raw_decode(stripped)
        except json.JSONDecodeError as err:
            if not final:
                return stripped

            raw_payload = stripped.strip()
            if raw_payload:
                yield raw_payload, None, str(err)
            return ""

        raw_payload = stripped[:end_idx].strip()
        if isinstance(event, dict):
            yield raw_payload, event, None
        else:
            yield raw_payload, None, "event payload must be a JSON object"
        buffer = stripped[end_idx:]


def _iter_jsonl_payloads(file_path: Path) -> Iterable[tuple[str, dict[str, Any] | None, str | None]]:
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
) -> Iterable[tuple[str, dict[str, Any] | None, str | None]]:
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
            buffer = yield from _drain_payload_buffer(
                buffer,
                decoder=decoder,
                final=False,
            )

    buffer = yield from _drain_payload_buffer(
        buffer,
        decoder=decoder,
        final=True,
    )


def iter_mooc_tracking_log_records(
    *,
    input_root: Path,
    max_files: int = 0,
    max_lines: int = 0,
    skip_decode_errors: bool = False,
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
    completed_files = 0

    for file_path in files:
        file_record_count = 0
        file_valid_count = 0
        file_decode_skipped_count = 0
        file_limit_reached = False

        for raw_payload, event, decode_error in _iter_payloads_from_file(file_path):
            if skip_decode_errors and decode_error is not None:
                file_decode_skipped_count += 1
                continue

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
                file_valid_count += 1
                if max_lines > 0 and produced_valid_records > max_lines:
                    file_limit_reached = True
                    break

            file_record_count += 1

            yield ReplayRecord(
                raw_line=raw_payload,
                event=event,
                key_bytes=key_bytes,
                event_time=event_time,
                decode_error=decode_error,
                validation_ok=validation_ok,
                validation_reason=validation_reason,
            )

        completed_files += 1
        print(
            f"completed_file={completed_files}/{len(files)} records={file_record_count} "
            f"valid_records={file_valid_count} decode_skipped={file_decode_skipped_count} "
            f"path={file_path}"
        )

        if file_limit_reached:
            return
