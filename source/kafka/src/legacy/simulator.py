from __future__ import annotations

import argparse
import time
import uuid

from kafka.src.common import RAW_TOPIC, delivery_report, encode_json, make_producer, utc_now_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Legacy simulator for compatibility.")
    parser.add_argument("--brokers", default="broker1:29092,broker2:29092,broker3:29092")
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--count", type=int, default=100)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    producer = make_producer(args.brokers)
    for idx in range(args.count):
        event = {"time": utc_now_iso(), "event_type": "legacy.simulated", "event_source": "simulator", "index": idx}
        key = str(uuid.uuid4())
        producer.produce(args.topic, key=key.encode("utf-8"), value=encode_json(event), on_delivery=delivery_report)
        producer.poll(0)
        time.sleep(0.01)
    producer.flush(10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
