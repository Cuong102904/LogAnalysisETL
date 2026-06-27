from __future__ import annotations

import argparse
from pathlib import Path

from confluent_kafka import Producer

from learnlake.replay import iter_tracking_log_records, sleep_by_event_delta
from learnlake.runtime import load_source_profile
from learnlake.runtime.config import resolve_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay static source events as raw JSON.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--input")
    parser.add_argument("--output", help="Optional JSONL sink used when Kafka publishing is not requested.")
    parser.add_argument("--brokers", help="Kafka bootstrap servers used when publishing to Kafka.")
    parser.add_argument("--topic", help="Kafka topic used when publishing to Kafka.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Replay records and print them to stdout without writing output or publishing to Kafka.",
    )
    parser.add_argument(
        "--dry-run-limit",
        type=int,
        default=20,
        help="Limit how many records are printed in dry-run mode. 0 means unlimited.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Limit the number of input files to replay. 0 means all files.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Limit the number of decoded records to replay. 0 means all records.",
    )
    parser.add_argument("--speed", type=float, default=0.0, help="0 means no pacing delay.")
    parser.add_argument(
        "--skip-decode-errors",
        action="store_true",
        help="Skip malformed replay records instead of failing the replay.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profile = load_source_profile(args.source)
    input_path = resolve_path(args.input or profile.input.path or "")
    output_path = Path(args.output).expanduser() if args.output else None
    topic = args.topic or profile.input.topic
    producer = None
    if not args.dry_run and args.brokers and topic:
        producer = Producer({"bootstrap.servers": args.brokers})
    previous_event_time = None
    if output_path and not args.dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        sink = output_path.open("w", encoding="utf-8")
    else:
        sink = None
    dry_run_printed = 0
    replayed_records = 0
    try:
        for record in iter_tracking_log_records(
            input_path,
            event_time_field=profile.input.event_time_field,
            max_files=args.max_files,
            skip_decode_errors=args.skip_decode_errors,
        ):
            if record.decode_error is not None and not args.skip_decode_errors:
                raise ValueError(
                    f"failed to decode replay record from {record.source_path}: {record.decode_error}"
                )

            observed = record.event_time
            sleep_by_event_delta(previous_event_time, observed, args.speed)
            previous_event_time = observed or previous_event_time

            line = record.raw_line
            if args.dry_run:
                event_time_text = observed.isoformat() if observed is not None else "unknown"
                print(f"[dry-run] {record.source_path.name} event_time={event_time_text} {line}")
                dry_run_printed += 1
                if args.dry_run_limit > 0 and dry_run_printed >= args.dry_run_limit:
                    break
            elif sink:
                sink.write(line + "\n")
            if producer:
                producer.produce(topic, value=line.encode("utf-8"))
                producer.poll(0)
            elif not args.dry_run:
                print(line)

            replayed_records += 1
            if args.max_records > 0 and replayed_records >= args.max_records:
                break
    finally:
        if producer:
            producer.flush()
        if sink:
            sink.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
