from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

from kafka.src.common import decode_json, validate_tracking_event
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


def iter_mooc_tracking_log_records(
    *,
    input_root: Path,
    max_files: int = 0,
    max_lines: int = 0,
) -> Iterable[ReplayRecord]:
    """
    Adapter responsibility:
    - discover tracking.log-*.json* files
    - read and parse each JSON line into (event, time, key)
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
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue

                decode_error: str | None = None
                event: dict[str, Any] | None = None
                event_time = None
                key_bytes: bytes | None = None
                validation_ok = False
                validation_reason = ""

                try:
                    event = decode_json(raw)
                except Exception as err:  # noqa: BLE001
                    decode_error = str(err)
                    # Keep structural validation as failed for decode errors.
                    validation_ok = False
                    validation_reason = "decode_json_failed"
                else:
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
                    raw_line=raw,
                    event=event,
                    key_bytes=key_bytes,
                    event_time=event_time,
                    decode_error=decode_error,
                    validation_ok=validation_ok,
                    validation_reason=validation_reason,
                )
