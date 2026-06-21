from __future__ import annotations

import argparse
from pathlib import Path

from kafka.src.adapters.mooc_tracking_log_adapter import iter_mooc_tracking_log_records
from kafka.src.common import (
    ANONYMOUS_RAW_TOPIC,
    DLQ_TOPIC,
    RAW_TOPIC,
    delivery_report,
    make_producer,
)
from kafka.src.common.core_producer import replay_stream
from kafka.src.filters.mooc_event_filter import is_event_allowed, load_producer_filter_config

DEFAULT_FILTER_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "producer_filter.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay BK_activity_logs_unzipped into Kafka raw topic."
    )
    parser.add_argument("--brokers", default="broker1:29092,broker2:29092,broker3:29092")
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument(
        "--anonymous-topic",
        default=ANONYMOUS_RAW_TOPIC,
        help="Secondary raw topic for allowlisted events missing both username and context.user_id. "
        "Set empty to disable and send everything identified to --topic.",
    )
    parser.add_argument("--dlq-topic", default=DLQ_TOPIC)
    parser.add_argument("--filter-config", default=str(DEFAULT_FILTER_CONFIG_PATH))
    parser.add_argument("--input-root", default="../BK_activity_logs_unzipped")
    parser.add_argument("--max-files", type=int, default=0, help="0 means all files")
    parser.add_argument("--max-lines", type=int, default=0, help="0 means all lines")
    parser.add_argument(
        "--skip-decode-errors",
        action="store_true",
        help="Ignore malformed JSON events instead of routing them to DLQ.",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=100.0,
        help="Replay speed factor from event time gaps. 100.0 is 100x faster than original pace.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.speed <= 0:
        raise ValueError("--speed must be greater than 0")
    anonymous_topic = (args.anonymous_topic or "").strip() or None

    input_root = Path(args.input_root).resolve()
    filter_cfg_path = Path(args.filter_config).expanduser().resolve()
    filter_cfg = load_producer_filter_config(filter_cfg_path)

    records = iter_mooc_tracking_log_records(
        input_root=input_root,
        max_files=args.max_files,
        max_lines=args.max_lines,
        skip_decode_errors=args.skip_decode_errors,
    )

    producer = make_producer(args.brokers)
    stats = replay_stream(
        records=records,
        producer=producer,
        raw_topic=args.topic,
        dlq_topic=args.dlq_topic,
        speed=args.speed,
        dlq_failed_topic=args.topic,
        dlq_publish_cb=None,
        filter_fn=is_event_allowed,
        filter_cfg=filter_cfg,
        delivery_report_cb=delivery_report,
        anonymous_topic=anonymous_topic,
    )

    print(
        "completed "
        f"sent_raw={stats['sent_raw']} sent_raw_anonymous={stats['sent_raw_anonymous']} "
        f"sent_dlq={stats['sent_dlq']} skipped_invalid={stats['skipped_invalid']} "
        f"anonymous_topic={anonymous_topic!r} filter_config={filter_cfg_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
