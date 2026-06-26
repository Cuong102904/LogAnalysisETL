from __future__ import annotations

import json
from types import SimpleNamespace

from learnlake.runtime.config import resolve_path
from projects.daotao_ai.gold.pipelines.runtime import (
    load_batch_inputs,
    load_stream_inputs,
    write_batch_output,
    write_stream_output,
)
from projects.daotao_ai.gold.serving_registry import GoldServingRegistry
from serving.superset.bootstrap.bootstrap_superset import (
    DASHBOARD_SURFACES,
    LIVE_OPS_SURFACE,
    build_bootstrap_registry,
    build_position_json,
)


class FakeReader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def format(self, value: str) -> FakeReader:
        self.calls.append(("format", value))
        return self

    def load(self, path: str) -> str:
        self.calls.append(("load", path))
        return path


class FakeSpark:
    def __init__(self) -> None:
        self.read = FakeReader()
        self.readStream = FakeReader()


class FakeWriter:
    def __init__(self) -> None:
        self.options: dict[str, object] = {}
        self.partition_cols: tuple[str, ...] = ()
        self.mode_value: str | None = None
        self.output_mode: str | None = None
        self.trigger_kwargs: dict[str, object] = {}
        self.query_name: str | None = None
        self.saved_path: str | None = None
        self.started = False

    def format(self, _: str) -> FakeWriter:
        return self

    def outputMode(self, value: str) -> FakeWriter:
        self.output_mode = value
        return self

    def option(self, key: str, value: object) -> FakeWriter:
        self.options[key] = value
        return self

    def trigger(self, **kwargs: object) -> FakeWriter:
        self.trigger_kwargs = kwargs
        return self

    def queryName(self, value: str) -> FakeWriter:
        self.query_name = value
        return self

    def partitionBy(self, *cols: str) -> FakeWriter:
        self.partition_cols = cols
        return self

    def mode(self, value: str) -> FakeWriter:
        self.mode_value = value
        return self

    def save(self, path: str) -> None:
        self.saved_path = path

    def start(self) -> str:
        self.started = True
        return "query"


class FakeDataFrame:
    def __init__(self) -> None:
        self._writer = FakeWriter()

    @property
    def write(self) -> FakeWriter:
        return self._writer

    @property
    def writeStream(self) -> FakeWriter:
        return self._writer


def _silver_config() -> SimpleNamespace:
    return SimpleNamespace(
        input_learning_path="/tmp/silver/learning",
        input_video_path="/tmp/silver/video",
        input_pdf_path="/tmp/silver/pdf",
        input_performance_path="/tmp/silver/performance",
        input_exam_attempts_path="/tmp/silver/exam_attempts",
        input_system_path="/tmp/silver/system",
        output_video_friction_signals_path="/tmp/gold/video_friction_signals",
        output_pdf_engagement_features_path="/tmp/gold/pdf_engagement_features",
        output_quiz_attempt_metrics_path="/tmp/gold/quiz_attempt_metrics",
        output_user_learning_profile_daily_path="/tmp/gold/user_learning_profile_daily",
        output_exam_integrity_signals_path="/tmp/gold/exam_integrity_signals",
        output_behavior_anomaly_signals_path="/tmp/gold/behavior_anomaly_signals",
    )


def test_gold_serving_registry_marks_modes_and_trigger_policy() -> None:
    registry = GoldServingRegistry.from_path(
        resolve_path("catalog/metrics/gold_dashboard_serving.yaml")
    )

    assert registry.output_names_by_mode("streaming") == (
        "video_friction_signals",
        "exam_integrity_signals",
        "behavior_anomaly_signals",
    )
    assert registry.output_names_by_mode("batch") == (
        "pdf_engagement_features",
        "quiz_attempt_metrics",
        "user_learning_profile_daily",
    )
    assert registry.outputs_by_mode("streaming")[0].trigger_policy["seconds"] == 30
    assert registry.outputs_by_mode("batch")[0].trigger_policy["schedule"] == "daily"


def test_stream_and_batch_input_loaders_only_read_silver_paths() -> None:
    config = _silver_config()
    spark = FakeSpark()

    stream_inputs = load_stream_inputs(spark, config, ("video", "system"))
    batch_inputs = load_batch_inputs(spark, config, ("pdf", "learning"))

    assert stream_inputs == {
        "video": "/tmp/silver/video",
        "system": "/tmp/silver/system",
    }
    assert batch_inputs == {
        "pdf": "/tmp/silver/pdf",
        "learning": "/tmp/silver/learning",
    }
    assert spark.read.calls == [
        ("format", "delta"),
        ("load", "/tmp/silver/pdf"),
        ("format", "delta"),
        ("load", "/tmp/silver/learning"),
    ]
    assert spark.readStream.calls == [
        ("format", "delta"),
        ("load", "/tmp/silver/video"),
        ("format", "delta"),
        ("load", "/tmp/silver/system"),
    ]


def test_stream_and_batch_writers_keep_partition_keys_stable() -> None:
    stream_df = FakeDataFrame()
    batch_df = FakeDataFrame()

    query = write_stream_output(
        stream_df,
        "/tmp/gold/video_friction_signals",
        "/tmp/checkpoints/video_friction_signals",
        "gold_video_friction_signals",
        30,
        ("event_date", "course_id"),
    )
    write_batch_output(
        batch_df,
        "/tmp/gold/pdf_engagement_features",
        ("event_date", "course_id"),
    )

    assert query == "query"
    assert stream_df.writeStream.partition_cols == ("event_date", "course_id")
    assert stream_df.writeStream.options["path"] == "/tmp/gold/video_friction_signals"
    assert stream_df.writeStream.options["checkpointLocation"] == "/tmp/checkpoints/video_friction_signals"
    assert stream_df.writeStream.trigger_kwargs == {"processingTime": "30 seconds"}
    assert stream_df.writeStream.query_name == "gold_video_friction_signals"
    assert batch_df.write.partition_cols == ("event_date", "course_id")
    assert batch_df.write.mode_value == "overwrite"
    assert batch_df.write.saved_path == "/tmp/gold/pdf_engagement_features"


def test_superset_dashboard_registry_and_tabs() -> None:
    assert [surface.title for surface in DASHBOARD_SURFACES] == [
        "Live Ops",
        "Learning Analytics",
    ]
    assert LIVE_OPS_SURFACE.refresh_frequency == 30
    assert [section.title for section in LIVE_OPS_SURFACE.sections] == [
        "Operational Overview",
        "Trend Watch",
        "Triage",
    ]
    assert LIVE_OPS_SURFACE.sections[0].charts[0].viz_type == "big_number_total"
    assert LIVE_OPS_SURFACE.sections[1].charts[0].viz_type == "echarts_timeseries_line"
    assert LIVE_OPS_SURFACE.sections[2].charts[0].viz_type == "pivot_table"

    payload = build_bootstrap_registry()
    assert payload["surfaces"][0]["title"] == "Live Ops"
    assert payload["surfaces"][1]["refresh_frequency"] == 0
    assert payload["surfaces"][0]["sections"][0]["charts"][0]["viz_type"] == "big_number_total"
    assert payload["surfaces"][1]["sections"][0]["charts"][0]["viz_type"] == "echarts_timeseries_line"

    charts_by_title = {
        "Active Alerts": {"id": 1, "uuid": "uuid-1", "slice_name": "Active Alerts"},
        "Open Anomalies": {"id": 2, "uuid": "uuid-2", "slice_name": "Open Anomalies"},
        "Exam Flags": {"id": 3, "uuid": "uuid-3", "slice_name": "Exam Flags"},
        "Video Friction Trend": {
            "id": 4,
            "uuid": "uuid-4",
            "slice_name": "Video Friction Trend",
        },
        "Exam Integrity Trend": {
            "id": 5,
            "uuid": "uuid-5",
            "slice_name": "Exam Integrity Trend",
        },
        "Behavior Heatmap": {"id": 6, "uuid": "uuid-6", "slice_name": "Behavior Heatmap"},
        "Alert Events": {"id": 7, "uuid": "uuid-7", "slice_name": "Alert Events"},
    }
    position = json.loads(build_position_json(LIVE_OPS_SURFACE.sections, charts_by_title))

    assert position["TABS_ID"]["children"] == ["TAB-1", "TAB-2", "TAB-3"]
    assert position["TAB-1"]["meta"]["title"] == "Operational Overview"
    assert position["TAB-2"]["meta"]["title"] == "Trend Watch"
    assert position["TAB-3"]["meta"]["title"] == "Triage"
