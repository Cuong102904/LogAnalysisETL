from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

try:
    from confluent_kafka import Producer

    HAS_CONFLUENT = True
except ModuleNotFoundError:  # pragma: no cover
    Producer = Any  # type: ignore[assignment]
    HAS_CONFLUENT = False

RAW_TOPIC = "mooc.raw.events"
DLQ_TOPIC = "mooc.dlq.events"


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        print(f"delivery failed: {err}")
        return
    print(f"delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def make_producer(brokers: str) -> Producer:
    if not HAS_CONFLUENT:
        raise RuntimeError("confluent_kafka is required to run producer/consumer services")
    return Producer({"bootstrap.servers": brokers, "enable.idempotence": True, "acks": "all", "retries": 10})


def encode_json(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def decode_json(raw: bytes | str | None) -> dict[str, Any]:
    if raw is None:
        raise ValueError("event payload is empty")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("event payload must be a JSON object")
    return payload


def validate_tracking_event(event: dict[str, Any]) -> tuple[bool, str]:
    required = ("time", "event_type", "event_source")
    for key in required:
        if key not in event:
            return False, f"missing required field: {key}"
    return True, ""
