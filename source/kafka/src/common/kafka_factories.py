from typing import Any

try:
    from confluent_kafka import Producer

    HAS_CONFLUENT = True
except ModuleNotFoundError:  # pragma: no cover
    Producer = Any  # type: ignore[assignment]
    HAS_CONFLUENT = False


def make_producer(brokers: str) -> Producer:
    if not HAS_CONFLUENT:
        raise RuntimeError("confluent_kafka is required to run producer/consumer services")
    return Producer({"bootstrap.servers": brokers, "enable.idempotence": True, "acks": "all", "retries": 10})
