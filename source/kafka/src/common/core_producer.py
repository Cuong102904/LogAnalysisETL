from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

from kafka.src.common.actor_identity import event_has_subject_identity
from kafka.src.common.encoding import encode_json
from kafka.src.common.time_utils import utc_now_iso
from kafka.src.models.replay_record import ReplayRecord
from kafka.src.producers.replayer.pacing import sleep_by_anchor_clock


def build_dlq_payload(
    *,
    ingest_time: str,
    error_type: str,
    error_message: str,
    raw_event: Any,
    failed_topic: str | None = None,
    failed_key: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ingest_time": ingest_time,
        "error_type": error_type,
        "error_message": error_message,
        "raw_event": raw_event,
    }
    if failed_topic is not None:
        payload["failed_topic"] = failed_topic
    if failed_key is not None:
        payload["failed_key"] = failed_key
    return payload


def _decode_key_bytes(key_bytes: bytes | None) -> str:
    if not key_bytes:
        return "missing-key"
    try:
        return key_bytes.decode("utf-8", errors="replace")
    except Exception:  # pragma: no cover
        return "missing-key"


def replay_stream(
    *,
    records: Iterable[ReplayRecord],
    producer,
    raw_topic: str,
    dlq_topic: str,
    speed: float,
    dlq_failed_topic: str | None,
    dlq_publish_cb: Callable[[Any], None] | None = None,
    filter_fn: Callable[[dict[str, Any], dict[str, Any]], tuple[bool, str]] | None = None,
    filter_cfg: dict[str, Any] | None = None,
    delivery_report_cb: Callable[[Any, Any], None] | None = None,
    anonymous_topic: str | None = None,
) -> dict[str, int]:
    """
    Core replay engine:
    - Pace by event_time (anchor = first non-null event_time); non-monotonic timestamps
      only shorten or skip sleeps (never drop or reorder messages).
    - Route allowed events with username or context.user_id to raw_topic.
    - Allowed events lacking both identifiers go to anonymous_topic when set (same value body as raw).
    - Route decode/validation/filter failures to dlq_topic with taxonomy.
    """

    first_event_time: datetime | None = None
    replay_start_monotonic: float | None = None

    stats = {
        "sent_raw": 0,
        "sent_raw_anonymous": 0,
        "sent_dlq": 0,
        "skipped_invalid": 0,
    }

    for rec in records:
        if rec.event_time is not None and first_event_time is None:
            first_event_time = rec.event_time
            replay_start_monotonic = time.monotonic()

        if first_event_time is not None and rec.event_time is not None:
            sleep_by_anchor_clock(
                first_event_time=first_event_time,
                current_event_time=rec.event_time,
                replay_start_monotonic=replay_start_monotonic,
                speed=speed,
            )

        key_str = _decode_key_bytes(rec.key_bytes)

        # Decode/validation failures are DLQ'ed with dedicated error types.
        if rec.decode_error is not None:
            ingest_time = utc_now_iso()
            payload = build_dlq_payload(
                ingest_time=ingest_time,
                error_type="decode_failed",
                error_message=f"decode failed: {rec.decode_error}",
                raw_event=rec.raw_line,
                failed_topic=dlq_failed_topic,
                failed_key=key_str,
            )
            producer.produce(
                dlq_topic,
                key=rec.key_bytes or key_str.encode("utf-8"),
                value=encode_json(payload),
                on_delivery=delivery_report_cb,
            )
            stats["sent_dlq"] += 1
            stats["skipped_invalid"] += 1
            producer.poll(0)
            continue

        if not rec.validation_ok:
            ingest_time = utc_now_iso()
            payload = build_dlq_payload(
                ingest_time=ingest_time,
                error_type="validation_failed",
                error_message=f"validation failed: {rec.validation_reason}",
                raw_event=rec.event or rec.raw_line,
                failed_topic=dlq_failed_topic,
                failed_key=key_str,
            )
            producer.produce(
                dlq_topic,
                key=rec.key_bytes or key_str.encode("utf-8"),
                value=encode_json(payload),
                on_delivery=delivery_report_cb,
            )
            stats["sent_dlq"] += 1
            stats["skipped_invalid"] += 1
            producer.poll(0)
            continue

        # Filter routing.
        assert rec.event is not None  # valid records always have an event dict
        allowed = False
        reason = "filtered_out"
        if filter_fn is not None and filter_cfg is not None:
            allowed, reason = filter_fn(rec.event, filter_cfg)

        if allowed:
            anon_target = anonymous_topic.strip() if isinstance(anonymous_topic, str) else ""
            split_anonymous = bool(anon_target) and not event_has_subject_identity(rec.event)
            out_topic = anon_target if split_anonymous else raw_topic
            producer.produce(
                out_topic,
                key=rec.key_bytes,
                value=rec.raw_line.encode("utf-8"),
                on_delivery=delivery_report_cb,
            )
            if split_anonymous:
                stats["sent_raw_anonymous"] += 1
            else:
                stats["sent_raw"] += 1
            producer.poll(0)
        else:
            ingest_time = utc_now_iso()
            snapshot = {}
            # Snapshot is expected to be handled by filter module; keep minimal here.
            if dlq_publish_cb is not None:
                dlq_publish_cb({"event": rec.event, "reason": reason})
            payload = build_dlq_payload(
                ingest_time=ingest_time,
                error_type="filtered_out",
                error_message=reason,
                raw_event=rec.event,
                failed_topic=dlq_failed_topic,
                failed_key=key_str,
            )
            # Snapshot enrich: include common fields if present.
            snapshot["event_type"] = str(rec.event.get("event_type", ""))
            snapshot["event_source"] = str(rec.event.get("event_source", ""))
            snapshot["name"] = str(rec.event.get("name", ""))
            ctx = rec.event.get("context") if isinstance(rec.event.get("context"), dict) else {}
            snapshot["context.path"] = str(ctx.get("path", ""))
            payload["error_message"] = f"{reason} | snapshot={snapshot}"
            producer.produce(
                dlq_topic,
                key=rec.key_bytes or key_str.encode("utf-8"),
                value=encode_json(payload),
                on_delivery=delivery_report_cb,
            )
            stats["sent_dlq"] += 1
            producer.poll(0)

    producer.flush(20)
    return stats


__all__ = ["ReplayRecord", "build_dlq_payload", "replay_stream"]
