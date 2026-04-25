from __future__ import annotations

import time
from datetime import datetime


def parse_event_time(raw_value: object) -> datetime | None:
    if not isinstance(raw_value, str):
        return None
    value = raw_value.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def sleep_for_event_gap(previous_time: datetime | None, current_time: datetime | None, speed: float) -> None:
    if previous_time is None or current_time is None:
        return
    if speed <= 0:
        return
    delta_seconds = (current_time - previous_time).total_seconds()
    if delta_seconds <= 0:
        return
    time.sleep(delta_seconds / speed)


def sleep_by_anchor_clock(
    first_event_time: datetime | None,
    current_event_time: datetime | None,
    replay_start_monotonic: float | None,
    speed: float,
) -> None:
    if first_event_time is None or current_event_time is None or replay_start_monotonic is None:
        return
    if speed <= 0:
        return
    event_elapsed_seconds = (current_event_time - first_event_time).total_seconds()
    if event_elapsed_seconds <= 0:
        return
    target_elapsed_seconds = event_elapsed_seconds / speed
    actual_elapsed_seconds = time.monotonic() - replay_start_monotonic
    sleep_seconds = target_elapsed_seconds - actual_elapsed_seconds
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
