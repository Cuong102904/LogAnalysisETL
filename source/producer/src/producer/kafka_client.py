from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        sys.stderr.write(f"delivery failed: {err}\n")
        return
    key_display = msg.key().decode("utf-8", errors="replace") if msg.key() else None
    print(
        f"delivered topic={msg.topic()} partition={msg.partition()} "
        f"offset={msg.offset()} key={key_display!r}"
    )


def make_producer(brokers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": brokers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "linger.ms": 20,
            "batch.num.messages": 1000,
            "compression.type": "snappy",
        }
    )


def event_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def parse_event_field(record: dict[str, Any]) -> Any:
    event_val = record.get("event")
    if isinstance(event_val, str):
        stripped = event_val.strip()
        if (stripped.startswith("{") and stripped.endswith("}")) or (
            stripped.startswith("[") and stripped.endswith("]")
        ):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return event_val
    return event_val


def ingest_ts() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def build_partition_key(record: dict[str, Any], key_mode: str) -> str:
    context = record.get("context") if isinstance(record.get("context"), dict) else {}
    if key_mode == "session":
        return str(record.get("session") or "no-session")
    course_id = str(context.get("course_id") or "no-course")
    username = str(record.get("username") or "no-user")
    return f"{course_id}|{username}"


def produce_json(
    producer: Producer,
    topic: str,
    key: str,
    value: dict[str, Any],
) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, ensure_ascii=False).encode("utf-8"),
        on_delivery=_delivery_report,
    )

