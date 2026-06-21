from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

from learnlake.normalization.values import parse_timestamp


def _window_start(value: datetime, minutes: int) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    minute = (value.minute // minutes) * minutes
    return value.replace(minute=minute, second=0, microsecond=0)


def build_course_activity_summary(
    events: Iterable[dict[str, Any]],
    *,
    window_minutes: int = 60,
) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str | None, datetime], dict[str, Any]] = {}
    learners: dict[tuple[str, str | None, datetime], set[str]] = defaultdict(set)

    for event in events:
        event_time = event["event_time"]
        if not isinstance(event_time, datetime):
            parsed_time = parse_timestamp(event_time)
            if parsed_time is None:
                continue
            event_time = parsed_time
        start = _window_start(event_time, window_minutes)
        key = (event["source_id"], event.get("course_id"), start)
        bucket = buckets.setdefault(
            key,
            {
                "source_id": event["source_id"],
                "course_id": event.get("course_id"),
                "window_start": start,
                "window_end": start + timedelta(minutes=window_minutes),
                "active_learners": 0,
                "learning_event_count": 0,
                "noise_event_count": 0,
                "unknown_event_count": 0,
                "total_event_count": 0,
            },
        )
        bucket["total_event_count"] += 1
        relevance = event.get("learning_relevance")
        actor_id = event.get("actor_id")
        if actor_id and event.get("is_authenticated") and not event.get("is_bot"):
            learners[key].add(str(actor_id))
        if relevance == "learning":
            bucket["learning_event_count"] += 1
        elif relevance == "noise":
            bucket["noise_event_count"] += 1
        elif relevance == "unknown":
            bucket["unknown_event_count"] += 1

    for key, bucket in buckets.items():
        bucket["active_learners"] = len(learners[key])
    return list(buckets.values())
