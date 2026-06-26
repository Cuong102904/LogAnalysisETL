from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from projects.daotao_ai.gold.serving_registry import (
    GoldOutputCadence,
    load_batch_inputs,
    load_stream_inputs,
    resolve_input_path,
    resolve_output_path,
    write_batch_output,
    write_stream_output,
)

__all__ = [
    "Any",
    "GoldOutputCadence",
    "load_batch_inputs",
    "load_stream_inputs",
    "resolve_input_path",
    "resolve_output_path",
    "write_batch_output",
    "write_stream_output",
    "run_periodic_job",
]


def run_periodic_job(
    job_name: str,
    interval_seconds: int,
    cycle: Callable[[], None],
) -> None:
    interval_seconds = max(1, int(interval_seconds))
    while True:
        started = time.monotonic()
        try:
            cycle()
        except Exception as exc:  # pragma: no cover - runtime guard
            print(f"{job_name} cycle failed: {exc}")
        elapsed = time.monotonic() - started
        sleep_for = max(0.0, interval_seconds - elapsed)
        if sleep_for:
            time.sleep(sleep_for)
