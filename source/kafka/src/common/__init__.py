from src.common.encoding import decode_json, encode_json
from src.common.kafka_factories import make_producer
from src.common.time_utils import utc_now_iso

RAW_TOPIC = "mooc.raw.events"
DLQ_TOPIC = "mooc.dlq.events"


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}")
        return
    print(f"delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def validate_tracking_event(event: dict) -> tuple[bool, str]:
    required = ("time", "event_type", "event_source")
    for key in required:
        if key not in event:
            return False, f"missing required field: {key}"
    return True, ""


__all__ = [
    "encode_json",
    "decode_json",
    "make_producer",
    "utc_now_iso",
    "RAW_TOPIC",
    "DLQ_TOPIC",
    "delivery_report",
    "validate_tracking_event",
]
