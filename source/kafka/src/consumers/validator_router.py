from __future__ import annotations

import argparse

from confluent_kafka import Consumer

from kafka.src.common import DLQ_TOPIC, RAW_TOPIC, decode_json, encode_json, make_producer, utc_now_iso, validate_tracking_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deprecated validator router kept for backward compatibility.")
    parser.add_argument(
        "--brokers",
        default="broker1:29092,broker2:29092,broker3:29092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument("--source-topic", default=RAW_TOPIC, help="Source topic to consume.")
    parser.add_argument("--group-id", default="mooc-validator-router", help="Consumer group id.")
    parser.add_argument("--dlq-topic", default=DLQ_TOPIC, help="DLQ topic.")
    return parser.parse_args()


def make_consumer(brokers: str, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )


def publish_dlq(producer, dlq_topic: str, key: str, reason: str, raw_event) -> None:
    payload = {
        "ingest_time": utc_now_iso(),
        "error_type": "validation_or_routing_failed",
        "error_message": reason,
        "failed_topic": dlq_topic,
        "failed_key": key,
        "raw_event": raw_event,
    }
    producer.produce(dlq_topic, key=key.encode("utf-8"), value=encode_json(payload))


def main() -> int:
    args = parse_args()
    consumer = make_consumer(args.brokers, args.group_id)
    producer = make_producer(args.brokers)
    consumer.subscribe([args.source_topic])
    print(f"subscribed topic={args.source_topic} group_id={args.group_id}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consume error: {msg.error()}")
                continue

            key = (
                msg.key().decode("utf-8", errors="replace")
                if msg.key()
                else "missing-key"
            )
            raw_value = msg.value()
            try:
                event = decode_json(raw_value)
            except Exception as err:  # noqa: BLE001
                publish_dlq(producer, args.dlq_topic, key, f"decode failed: {err}", raw_value.decode("utf-8", errors="replace") if raw_value else None)
                producer.poll(0)
                continue

            valid, reason = validate_tracking_event(event)
            if not valid:
                publish_dlq(producer, args.dlq_topic, key, reason, event)
                producer.poll(0)
                continue

            publish_dlq(producer, args.dlq_topic, key, "validator_router is deprecated; use spark silver classifier", event)
            producer.poll(0)
    except KeyboardInterrupt:
        print("shutdown requested")
    finally:
        producer.flush(10)
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
