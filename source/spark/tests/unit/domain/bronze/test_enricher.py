from datetime import datetime

from domain.bronze.enricher import enrich_bronze
from domain.bronze.time import parse_raw_event_time
from pyspark.sql import functions as F


def test_parse_raw_event_time_parses_valid_iso8601(spark) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    result = (
        spark.range(1)
        .select(
            parse_raw_event_time(F.lit('{"time":"2026-01-17T21:25:06.203128+00:00"}')).alias("time")
        )
        .collect()
    )

    assert result[0]["time"] == datetime(2026, 1, 18, 4, 25, 6, 203128)


def test_parse_raw_event_time_returns_null_for_invalid_payload(spark) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    result = (
        spark.range(1)
        .select(parse_raw_event_time(F.lit('{"time":"not-a-timestamp"}')).alias("time"))
        .collect()
    )

    assert result[0]["time"] is None


def test_enrich_bronze_maps_time_and_keeps_canonical_columns(spark) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    raw = spark.createDataFrame(
        [
            {
                "topic": "mooc.raw.events",
                "partition": 2,
                "offset": 44,
                "key": "user_001",
                "value": '{"time":"2026-01-17T21:25:06.203128+00:00","event_type":"play_video"}',
            }
        ]
    )

    result = enrich_bronze(raw).collect()[0]

    assert result["time"] == datetime(2026, 1, 18, 4, 25, 6, 203128)
    assert set(result.asDict().keys()) == {
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "time",
        "kafka_key",
        "value_raw",
        "ingest_ts",
        "ingest_date",
        "ingest_hour",
        "dedup_key",
    }
