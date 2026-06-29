from __future__ import annotations

import json
import os

import pytest
from pyspark.sql.types import StringType

from learnlake.ingestion.bronze_transform import transform_bronze_dataframe
from learnlake.runtime import build_spark, load_source_profile


@pytest.fixture(scope="session")
def spark():
    previous_openlineage = os.environ.get("OPENLINEAGE_ENABLED")
    os.environ["OPENLINEAGE_ENABLED"] = "false"
    session = build_spark("test_bronze_transform")
    try:
        yield session
    finally:
        session.stop()
        if previous_openlineage is None:
            os.environ.pop("OPENLINEAGE_ENABLED", None)
        else:
            os.environ["OPENLINEAGE_ENABLED"] = previous_openlineage


def test_transform_bronze_dataframe_preserves_raw_payload_string(spark) -> None:
    profile = load_source_profile("daotao_ai")
    payload = {
        "time": "2026-01-17T21:22:17.049637+00:00",
        "event_type": "/courses/course-v1:SoDiTEC+Dsa01+2025_1/course/",
        "context": {"course_id": "course-v1:SoDiTEC+Dsa01+2025_1"},
    }
    source_df = spark.createDataFrame(
        [
            {
                "raw_json": json.dumps(payload),
                "kafka_topic": "mooc.raw.events",
                "kafka_partition": 2,
                "kafka_offset": 99,
            }
        ]
    )

    bronze_df = transform_bronze_dataframe(source_df, profile)
    row = bronze_df.collect()[0]

    assert bronze_df.schema["raw_payload"].dataType == StringType()
    assert row.raw_payload == json.dumps(payload)
    assert row.event_time_raw == payload["time"]
    assert row.source_event_type == payload["event_type"]
    assert row.kafka_topic == "mooc.raw.events"
