from __future__ import annotations

import time
from datetime import datetime


def sleep_by_event_delta(
    previous_event_time: datetime | None,
    current_event_time: datetime | None,
    speed: float,
) -> None:
    if previous_event_time is None or current_event_time is None:
        return
    if speed <= 0:
        return

    delta_seconds = (current_event_time - previous_event_time).total_seconds()
    if delta_seconds <= 0:
        return

    sleep_seconds = delta_seconds / speed
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
