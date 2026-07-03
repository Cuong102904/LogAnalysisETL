from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from confluent_kafka import Producer

from learnlake.replay import iter_tracking_log_records, sleep_by_event_delta
from learnlake.runtime import load_source_profile
from learnlake.runtime.config import resolve_path

DEFAULT_DLQ_TOPIC = "mooc.dlq.events"


def _produce_with_backpressure(producer: Producer, topic: str, value: bytes) -> None:
    while True:
        try:
            producer.produce(topic, value=value)
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.1)
            time.sleep(0.1)


def _publish_dlq_record(
    producer: Producer,
    topic: str,
    *,
    record_path: Path,
    raw_line: str,
    decode_error: str,
) -> None:
    payload = {
        "error_type": "decode_failed",
        "error_message": decode_error,
        "raw_line": raw_line,
        "source_path": str(record_path),
    }
    _produce_with_backpressure(producer, topic, json.dumps(payload, ensure_ascii=False).encode("utf-8"))


def _wait_for_kafka_bootstrap(
    producer: Producer,
    brokers: str,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = "Kafka bootstrap is not ready yet"

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Kafka bootstrap {brokers} did not become ready within {timeout_seconds:.1f}s: {last_error}"
            )

        probe_timeout = min(5.0, max(0.5, remaining))
        try:
            producer.list_topics(timeout=probe_timeout)
            return
        except Exception as exc:  # pragma: no cover - librdkafka error text varies by environment
            last_error = str(exc)
            print(
                f"Waiting for Kafka bootstrap {brokers} to become ready: {last_error}",
                flush=True,
            )
            time.sleep(poll_interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay static source events as raw JSON.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--input")
    parser.add_argument("--output", help="Optional JSONL sink used when Kafka publishing is not requested.")
    parser.add_argument("--brokers", help="Kafka bootstrap servers used when publishing to Kafka.")
    parser.add_argument("--topic", help="Kafka topic used when publishing to Kafka.")
    parser.add_argument(
        "--dlq-topic",
        default=DEFAULT_DLQ_TOPIC,
        help="Kafka topic used for parser/decode failures. Set empty to disable DLQ publishing.",
    )
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
    parser.add_argument(
        "--bootstrap-timeout",
        type=float,
        default=120.0,
        help="Maximum number of seconds to wait for Kafka bootstrap to accept metadata requests.",
    )
    parser.add_argument(
        "--bootstrap-poll-interval",
        type=float,
        default=2.0,
        help="Seconds between Kafka readiness checks.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profile = load_source_profile(args.source)
    input_path = resolve_path(args.input or profile.input.path or "")
    output_path = Path(args.output).expanduser() if args.output else None
    topic = args.topic or profile.input.topic
    dlq_topic = (args.dlq_topic or "").strip() or None
    producer = None
    if not args.dry_run and args.brokers and (topic or dlq_topic):
        producer_config = {
            "bootstrap.servers": args.brokers,
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 1,
        }
        producer = Producer(producer_config)
        _wait_for_kafka_bootstrap(
            producer,
            args.brokers,
            timeout_seconds=args.bootstrap_timeout,
            poll_interval_seconds=args.bootstrap_poll_interval,
        )
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
            if record.decode_error is not None:
                if producer and dlq_topic and not args.dry_run:
                    _publish_dlq_record(
                        producer,
                        dlq_topic,
                        record_path=record.source_path,
                        raw_line=record.raw_line,
                        decode_error=record.decode_error,
                    )
                    continue

                if not args.skip_decode_errors:
                    raise ValueError(
                        f"failed to decode replay record from {record.source_path}: {record.decode_error}"
                    )
                continue

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
            if producer and topic:
                _produce_with_backpressure(producer, topic, line.encode("utf-8"))
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
