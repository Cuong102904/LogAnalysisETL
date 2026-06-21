from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from learnlake.contracts import BronzeEnvelope, SourceProfile
from learnlake.normalization.values import get_path, parse_timestamp


def _stable_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def build_bronze_envelope(
    payload: dict[str, Any],
    profile: SourceProfile,
    *,
    ingestion_time: datetime | None = None,
    kafka_topic: str | None = None,
    kafka_partition: int | None = None,
    kafka_offset: int | None = None,
) -> BronzeEnvelope:
    observed_ingestion_time = ingestion_time or datetime.now(timezone.utc)
    event_time_raw_value = get_path(payload, profile.input.event_time_field)
    event_time_raw = None if event_time_raw_value is None else str(event_time_raw_value)
    event_type = (
        get_path(payload, profile.input.event_type_field)
        if profile.input.event_type_field
        else None
    )
    event_basis = "|".join(
        [
            profile.source_id,
            event_time_raw or "",
            str(event_type or ""),
            _stable_payload(payload),
            str(kafka_topic or ""),
            str(kafka_partition if kafka_partition is not None else ""),
            str(kafka_offset if kafka_offset is not None else ""),
        ]
    )
    event_id = hashlib.sha256(event_basis.encode("utf-8")).hexdigest()
    return BronzeEnvelope(
        event_id=event_id,
        source_id=profile.source_id,
        source_type=profile.source_type,
        source_event_type=None if event_type is None else str(event_type),
        event_time_raw=event_time_raw,
        event_time=parse_timestamp(event_time_raw),
        ingestion_time=observed_ingestion_time,
        raw_payload=payload,
        kafka_topic=kafka_topic,
        kafka_partition=kafka_partition,
        kafka_offset=kafka_offset,
        processing_date=observed_ingestion_time.date(),
    )


def build_bronze_records(
    payloads: Iterable[dict[str, Any]],
    profile: SourceProfile,
    *,
    ingestion_time: datetime | None = None,
) -> list[dict[str, Any]]:
    return [
        build_bronze_envelope(payload, profile, ingestion_time=ingestion_time).as_record()
        for payload in payloads
    ]
