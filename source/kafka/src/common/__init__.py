from kafka.src.common.encoding import decode_json, encode_json
from kafka.src.common.kafka_factories import make_producer
from kafka.src.common.time_utils import utc_now_iso

# Canonical Kafka topics for replay output (same JSON line value encoding for both raw streams).
#
# RAW_TOPIC — allowlisted LMS rows linked to an identifiable learner: non-empty username and/or
# numeric context.user_id > 0 (see actor_identity.event_has_subject_identity).
RAW_TOPIC = "mooc.raw.events"
#
# ANONYMOUS_RAW_TOPIC — passes the same allowlist and payload shape as RAW_TOPIC, but the row has
# neither username nor usable user_id (anonymous / crawler / tooling); still persisted, not DLQ.
ANONYMOUS_RAW_TOPIC = "mooc.raw.anonymous.events"
#
# DLQ_TOPIC — malformed JSON, failed minimal validation, or filtered_out events (wrapped DLQ JSON).
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
    "ANONYMOUS_RAW_TOPIC",
    "DLQ_TOPIC",
    "delivery_report",
    "validate_tracking_event",
]
