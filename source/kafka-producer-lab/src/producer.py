"""Kafka producer for real tracking logs with topic routing."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from confluent_kafka import KafkaException, Producer

classify_event = importlib.import_module("event_classifier").classify_event


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        sys.stderr.write(f"delivery failed: {err}\n")
        return
    key_display = msg.key().decode("utf-8", errors="replace") if msg.key() else None
    print(
        f"delivered topic={msg.topic()} partition={msg.partition()} "
        f"offset={msg.offset()} key={key_display!r}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest JSONL tracking logs to Kafka and route by classification.",
    )
    parser.add_argument(
        "--brokers",
        default="localhost:9092,localhost:9093,localhost:9094",
        help="Comma-separated bootstrap servers (host from host machine).",
    )
    parser.add_argument(
        "--input-file",
        required=True,
        help="Path to one tracking.log-* JSONL file.",
    )
    parser.add_argument(
        "--raw-topic",
        default="lms.raw.events",
        help="Topic for full raw records.",
    )
    parser.add_argument(
        "--exam-topic",
        default="lms.exam.events",
        help="Topic for exam/proctoring events.",
    )
    parser.add_argument(
        "--learning-topic",
        default="lms.learning.events",
        help="Topic for learning events.",
    )
    parser.add_argument(
        "--noise-topic",
        default="lms.noise.events",
        help="Topic for noise/bot events.",
    )
    parser.add_argument(
        "--dlq-topic",
        default="lms.dlq.events",
        help="Topic for malformed lines and parse failures.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Stop after N records (0 means process all lines).",
    )
    parser.add_argument(
        "--key-mode",
        choices=("course_user", "session"),
        default="course_user",
        help="Partition key policy.",
    )
    return parser.parse_args()


def _iter_lines(path: Path, max_records: int) -> Iterable[tuple[int, str]]:
    with path.open("r", encoding="utf-8") as fh:
        for idx, line in enumerate(fh, start=1):
            if max_records and idx > max_records:
                break
            text = line.strip()
            if text:
                yield idx, text


def _event_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _parse_event_field(record: dict[str, Any]) -> Any:
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


def _build_partition_key(record: dict[str, Any], key_mode: str) -> str:
    context = record.get("context") if isinstance(record.get("context"), dict) else {}
    if key_mode == "session":
        return str(record.get("session") or "no-session")
    course_id = str(context.get("course_id") or "no-course")
    username = str(record.get("username") or "no-user")
    return f"{course_id}|{username}"


def _target_topic(label: str, args: argparse.Namespace) -> str:
    if label == "exam":
        return args.exam_topic
    if label == "learning":
        return args.learning_topic
    if label == "noise":
        return args.noise_topic
    return args.learning_topic


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_file)
    if not input_path.exists():
        sys.stderr.write(f"input file not found: {input_path}\n")
        return 1

    producer = Producer(
        {
            "bootstrap.servers": args.brokers,
            "client.id": "kafka-producer-lab-ingest",
        }
    )

    counters: Counter[str] = Counter()
    source_file = input_path.name

    try:
        for line_no, raw_line in _iter_lines(input_path, args.max_records):
            counters["lines_seen"] += 1
            try:
                record = json.loads(raw_line)
                if not isinstance(record, dict):
                    raise ValueError("json line is not an object")
            except Exception as exc:
                dlq_payload = {
                    "ingest_ts": datetime.now(timezone.utc).isoformat(),
                    "source_file": source_file,
                    "line_no": line_no,
                    "error": str(exc),
                    "raw_line": raw_line,
                }
                producer.produce(
                    args.dlq_topic,
                    key=f"dlq|{source_file}".encode("utf-8"),
                    value=json.dumps(dlq_payload, ensure_ascii=False).encode("utf-8"),
                    callback=_delivery_report,
                )
                counters["dlq"] += 1
                producer.poll(0)
                continue

            record["event"] = _parse_event_field(record)
            result = classify_event(record)
            key = _build_partition_key(record, args.key_mode)
            envelope = {
                "ingest_ts": datetime.now(timezone.utc).isoformat(),
                "source_file": source_file,
                "line_no": line_no,
                "classification": result.label,
                "classification_reason": result.reason,
                "event_hash": _event_hash(raw_line),
                "payload": record,
            }
            value = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
            key_bytes = key.encode("utf-8")

            producer.produce(
                args.raw_topic,
                key=key_bytes,
                value=value,
                callback=_delivery_report,
            )
            producer.produce(
                _target_topic(result.label, args),
                key=key_bytes,
                value=value,
                callback=_delivery_report,
            )
            counters["produced_raw"] += 1
            counters[f"class_{result.label}"] += 1
            producer.poll(0)

        producer.flush()
    except KafkaException as exc:
        sys.stderr.write(f"kafka error: {exc}\n")
        return 1

    print("ingest_summary")
    print(f"  lines_seen={counters['lines_seen']}")
    print(f"  produced_raw={counters['produced_raw']}")
    print(f"  class_exam={counters['class_exam']}")
    print(f"  class_learning={counters['class_learning']}")
    print(f"  class_noise={counters['class_noise']}")
    print(f"  class_other={counters['class_other']}")
    print(f"  dlq={counters['dlq']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
