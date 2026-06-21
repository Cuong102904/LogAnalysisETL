from pyspark.sql.types import LongType, StringType, StructField, StructType

TRACKING_EVENT_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField(
            "context",
            StructType(
                [
                    StructField("user_id", LongType(), True),
                    StructField("path", StringType(), True),
                    StructField("course_id", StringType(), True),
                    StructField("org_id", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("username", StringType(), True),
        StructField("session", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("agent", StringType(), True),
        StructField("host", StringType(), True),
        StructField("referer", StringType(), True),
        StructField("accept_language", StringType(), True),
        StructField("event", StringType(), True),
        StructField("time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_source", StringType(), True),
        StructField("page", StringType(), True),
    ]
)
