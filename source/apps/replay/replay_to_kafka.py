from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from learnlake.connectors import read_json_lines
from learnlake.normalization.values import parse_timestamp
from learnlake.runtime import load_source_profile
from learnlake.runtime.config import resolve_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay static source events as raw JSON.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--input")
    parser.add_argument("--output", help="Optional JSONL sink used when Kafka publishing is not requested.")
    parser.add_argument("--brokers", help="Kafka bootstrap servers used when publishing to Kafka.")
    parser.add_argument("--topic", help="Kafka topic used when publishing to Kafka.")
    parser.add_argument("--speed", type=float, default=0.0, help="0 means no pacing delay.")
    return parser.parse_args()


def _event_time(record: dict[str, Any], field: str):
    current: Any = record
    for part in field.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return parse_timestamp(current)


def _read_input_records(path: Path) -> list[dict[str, Any]]:
    if path.is_dir():
        records: list[dict[str, Any]] = []
        for file_path in sorted(path.rglob("*.jsonl")):
            records.extend(read_json_lines(file_path))
        return records
    return list(read_json_lines(path))


def main() -> int:
    args = parse_args()
    profile = load_source_profile(args.source)
    input_path = resolve_path(args.input or profile.input.path or "")
    records = _read_input_records(input_path)
    records.sort(key=lambda item: _event_time(item, profile.input.event_time_field) or 0)
    output_path = Path(args.output).expanduser() if args.output else None
    topic = args.topic or profile.input.topic
    producer = Producer({"bootstrap.servers": args.brokers}) if args.brokers and topic else None
    previous = None
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        sink = output_path.open("w", encoding="utf-8")
    else:
        sink = None
    try:
        for record in records:
            observed = _event_time(record, profile.input.event_time_field)
            if args.speed > 0 and observed and previous:
                delay = max((observed - previous).total_seconds() / args.speed, 0)
                time.sleep(delay)
            previous = observed or previous
            line = json.dumps(record, sort_keys=True)
            if sink:
                sink.write(line + "\n")
            if producer:
                producer.produce(topic, value=line.encode("utf-8"))
                producer.poll(0)
            else:
                print(line)
    finally:
        if producer:
            producer.flush()
        if sink:
            sink.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
