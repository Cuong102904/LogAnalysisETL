from __future__ import annotations

import argparse
import time
from pathlib import Path

from kafka.src.common import RAW_TOPIC, decode_json, delivery_report, make_producer, validate_tracking_event
from kafka.src.producers.replayer import parse_event_time, sleep_by_anchor_clock


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay BK_activity_logs_unzipped into Kafka raw topic.")
    parser.add_argument("--brokers", default="broker1:29092,broker2:29092,broker3:29092")
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--input-root", default="../BK_activity_logs_unzipped")
    parser.add_argument("--max-files", type=int, default=0, help="0 means all files")
    parser.add_argument("--max-lines", type=int, default=0, help="0 means all lines")
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Replay speed factor from event time gaps. 1.0 is original speed, 2.0 is 2x faster.",
    )
    return parser.parse_args()


def _iter_files(root: Path) -> list[Path]:
    return [p for p in root.rglob("tracking.log-*.json*") if p.is_file()]


def _extract_key(event: dict) -> str:
    username = str(event.get("username") or "").strip()
    session = str(event.get("session") or "").strip()
    ip = str(event.get("ip") or "").strip()
    if username:
        return f"user:{username}"
    if session:
        return f"session:{session}"
    return f"ip:{ip or 'unknown'}"


def main() -> int:
    args = parse_args()
    if args.speed <= 0:
        raise ValueError("--speed must be greater than 0")
    root = Path(args.input_root).resolve()
    files = _iter_files(root)
    if args.max_files > 0:
        files = files[: args.max_files]
    producer = make_producer(args.brokers)
    sent = 0
    skipped = 0
    first_event_time = None
    replay_start_monotonic = None
    for file_path in files:
        with file_path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    event = decode_json(raw)
                except Exception:
                    skipped += 1
                    continue
                valid, _ = validate_tracking_event(event)
                if not valid:
                    skipped += 1
                    continue
                event_time = parse_event_time(event.get("time"))
                if event_time is not None and first_event_time is None:
                    first_event_time = event_time
                    replay_start_monotonic = time.monotonic()
                sleep_by_anchor_clock(first_event_time, event_time, replay_start_monotonic, args.speed)
                key = _extract_key(event).encode("utf-8")
                producer.produce(args.topic, key=key, value=raw.encode("utf-8"), on_delivery=delivery_report)
                producer.poll(0)
                sent += 1
                if args.max_lines > 0 and sent >= args.max_lines:
                    producer.flush(20)
                    print(f"sent={sent} skipped={skipped}")
                    return 0
    producer.flush(20)
    print(f"completed sent={sent} skipped={skipped} files={len(files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
