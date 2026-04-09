from __future__ import annotations

from pyspark.sql.types import (
    MapType,
    StringType,
    StructField,
    StructType,
)


CANONICAL_EVENT_SCHEMA = StructType(
    [
        StructField("event_source", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("time", StringType(), True),
        StructField("username", StringType(), True),
        StructField("session", StringType(), True),
        StructField("course_id", StringType(), True),
        StructField("org_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("path", StringType(), True),
        # Keep event payload as map<string,string> for skeleton simplicity
        StructField("event", MapType(StringType(), StringType(), True), True),
    ]
)

