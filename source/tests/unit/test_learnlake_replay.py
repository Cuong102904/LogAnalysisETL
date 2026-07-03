from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from apps.replay import replay_to_kafka
from learnlake.replay.pacing import sleep_by_event_delta
from learnlake.replay.tracking import iter_tracking_log_records


def _write_jsonl(path, records) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(record, sort_keys=True) for record in records) + "\n",
        encoding="utf-8",
    )


def test_tracking_replay_yields_files_in_sorted_order(tmp_path) -> None:
    later_file = tmp_path / "normal_days" / "tracking.log-20260119-1768785421.json"
    earlier_file = tmp_path / "exam_days" / "tracking.log-20260118-1768713421.json"

    _write_jsonl(
        later_file,
        [
            {
                "time": "2026-01-19T00:00:00Z",
                "event_type": "later-1",
                "event_source": "server",
            },
            {
                "time": "2026-01-19T00:00:05Z",
                "event_type": "later-2",
                "event_source": "server",
            },
        ],
    )
    _write_jsonl(
        earlier_file,
        [
            {
                "time": "2026-01-18T00:00:00Z",
                "event_type": "earlier-1",
                "event_source": "server",
            },
            {
                "time": "2026-01-18T00:00:05Z",
                "event_type": "earlier-2",
                "event_source": "server",
            },
        ],
    )

    records = list(iter_tracking_log_records(tmp_path, event_time_field="time"))

    assert [record.source_path.name for record in records] == [
        earlier_file.name,
        earlier_file.name,
        later_file.name,
        later_file.name,
    ]
    assert [record.event["event_type"] for record in records] == [
        "earlier-1",
        "earlier-2",
        "later-1",
        "later-2",
    ]


def test_tracking_replay_respects_max_lines_after_valid_records(tmp_path) -> None:
    first_file = tmp_path / "exam_days" / "tracking.log-20260118-1768713421.json"
    second_file = tmp_path / "normal_days" / "tracking.log-20260119-1768785421.json"

    first_file.parent.mkdir(parents=True, exist_ok=True)
    first_file.write_text(
        '{"time":"2026-01-18T00:00:00Z","event_type":"first","event_source":"server"}\n'
        'not-json\n'
        '{"time":"2026-01-18T00:00:05Z","event_type":"first-2","event_source":"server"}\n',
        encoding="utf-8",
    )
    _write_jsonl(
        second_file,
        [
            {
                "time": "2026-01-19T00:00:00Z",
                "event_type": "second-1",
                "event_source": "server",
            }
        ],
    )

    records = list(
        iter_tracking_log_records(
            tmp_path,
            event_time_field="time",
            max_lines=1,
            skip_decode_errors=True,
        )
    )

    assert len(records) == 1
    assert records[0].event["event_type"] == "first"
    assert records[0].event_time == datetime(2026, 1, 18, 0, 0, tzinfo=timezone.utc)


def test_tracking_replay_respects_max_files(tmp_path) -> None:
    first_file = tmp_path / "exam_days" / "tracking.log-20260118-1768713421.json"
    second_file = tmp_path / "normal_days" / "tracking.log-20260119-1768785421.json"

    _write_jsonl(
        first_file,
        [
            {
                "time": "2026-01-18T00:00:00Z",
                "event_type": "first",
                "event_source": "server",
            }
        ],
    )
    _write_jsonl(
        second_file,
        [
            {
                "time": "2026-01-19T00:00:00Z",
                "event_type": "second",
                "event_source": "server",
            }
        ],
    )

    records = list(
        iter_tracking_log_records(
            tmp_path,
            event_time_field="time",
            max_files=1,
        )
    )

    assert len(records) == 1
    assert records[0].source_path.name == first_file.name
    assert records[0].event["event_type"] == "first"


def test_sleep_by_event_delta_scales_sleep_seconds(monkeypatch) -> None:
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("learnlake.replay.pacing.time.sleep", fake_sleep)

    previous = datetime(2026, 1, 18, 0, 0, tzinfo=timezone.utc)
    current = datetime(2026, 1, 18, 0, 0, 20, tzinfo=timezone.utc)

    sleep_by_event_delta(previous, current, speed=4.0)

    assert sleeps == [5.0]


def test_replay_to_kafka_dry_run_prints_preview_without_side_effects(
    tmp_path, monkeypatch, capsys
) -> None:
    input_file = tmp_path / "exam_days" / "tracking.log-20260118-1768713421.json"
    _write_jsonl(
        input_file,
        [
            {
                "time": "2026-01-18T00:00:00Z",
                "event_type": "first",
                "event_source": "server",
            },
            {
                "time": "2026-01-18T00:00:05Z",
                "event_type": "second",
                "event_source": "server",
            }
        ],
    )

    monkeypatch.setattr(
        replay_to_kafka,
        "parse_args",
        lambda: SimpleNamespace(
            source="daotao_ai",
            input=str(tmp_path),
            output=str(tmp_path / "out.jsonl"),
            brokers="localhost:9092",
            topic="raw-topic",
            dlq_topic="",
            dry_run=True,
            dry_run_limit=1,
            max_files=0,
            speed=0.0,
            skip_decode_errors=False,
        ),
    )
    monkeypatch.setattr(
        replay_to_kafka,
        "load_source_profile",
        lambda _source: SimpleNamespace(
            input=SimpleNamespace(path=str(tmp_path), topic="raw-topic", event_time_field="time")
        ),
    )
    monkeypatch.setattr(replay_to_kafka, "resolve_path", lambda value: Path(value))
    monkeypatch.setattr(
        replay_to_kafka,
        "Producer",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Producer must not be created")),
    )

    assert replay_to_kafka.main() == 0

    captured = capsys.readouterr()
    assert captured.out.count("[dry-run]") == 1
    assert "event_type" in captured.out
    assert "second" not in captured.out
    assert not (tmp_path / "out.jsonl").exists()


def test_wait_for_kafka_bootstrap_retries_until_ready(monkeypatch) -> None:
    attempts: list[float] = []
    sleeps: list[float] = []

    class FakeProducer:
        def list_topics(self, timeout: float):
            attempts.append(timeout)
            if len(attempts) < 3:
                raise RuntimeError("transport failure")
            return SimpleNamespace(brokers={"broker1": object()})

    monkeypatch.setattr(replay_to_kafka.time, "sleep", lambda seconds: sleeps.append(seconds))

    replay_to_kafka._wait_for_kafka_bootstrap(
        FakeProducer(),
        "broker1:29092,broker2:29092,broker3:29092",
        timeout_seconds=5.0,
        poll_interval_seconds=0.25,
    )

    assert len(attempts) == 3
    assert sleeps == [0.25, 0.25]
