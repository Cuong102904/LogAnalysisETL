from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from producer.kafka_client import (
    build_partition_key,
    event_hash,
    ingest_ts,
    make_producer,
    produce_json,
)
from producer.reader import LogEvent, load_and_sort_events


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay BK_activity_logs_unzipped into Kafka by timestamp.")
    p.add_argument(
        "--brokers",
        default="localhost:9092,localhost:9093,localhost:9094",
        help="Kafka bootstrap servers reachable from host.",
    )
    p.add_argument(
        "--data-dir",
        required=True,
        help="Root directory containing tracking.log-* files (recursive).",
    )
    p.add_argument("--raw-topic", default="lms.raw.events")
    p.add_argument("--dlq-topic", default="lms.dlq.events")
    p.add_argument("--speed", type=float, default=60.0, help="Replay speed factor (60 = 1 min log time per 1 sec).")
    p.add_argument("--max-events", type=int, default=0, help="Stop after N events (0 = all loaded).")
    p.add_argument("--key-mode", choices=("course_user", "session"), default="course_user")
    p.add_argument("--no-sleep", action="store_true", help="Disable replay delay; send as fast as possible.")
    return p.parse_args()


def replay_delay(prev_ts: datetime, cur_ts: datetime, speed: float) -> float:
    if speed <= 0:
        return 0.0
    delta = (cur_ts - prev_ts).total_seconds()
    if delta <= 0:
        return 0.0
    return delta / speed


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        raise SystemExit(f"data dir not found: {data_dir}")

    events: list[LogEvent] = load_and_sort_events(data_dir, max_events_total=args.max_events)
    if not events:
        print("No events found.")
        return 0

    producer = make_producer(args.brokers)

    prev_ts = events[0].ts
    for idx, ev in enumerate(events, start=1):
        if args.max_events and idx > args.max_events:
            break

        if not args.no_sleep:
            time.sleep(replay_delay(prev_ts, ev.ts, args.speed))
        prev_ts = ev.ts

        if ev.record is None:
            dlq = {
                "ingest_ts": ingest_ts(),
                "source_path": str(ev.source_path),
                "line_no": ev.line_no,
                "raw_line": ev.raw_line,
                "error": "json_parse_failed_or_not_object",
            }
            produce_json(producer, args.dlq_topic, key=str(ev.line_no), value=dlq)
            producer.poll(0)
            continue

        raw = {
            "ingest_ts": ingest_ts(),
            "event_hash": event_hash(ev.raw_line),
            "source_path": str(ev.source_path),
            "line_no": ev.line_no,
            "record": ev.record,
        }
        key = build_partition_key(ev.record, args.key_mode)
        produce_json(producer, args.raw_topic, key=key, value=raw)

        producer.poll(0)

    producer.flush(30)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

