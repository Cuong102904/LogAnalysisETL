from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

USER_LEARNING_PROFILE_DAILY_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), True),
        StructField("course_id", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("event_count", LongType(), True),
        StructField("distinct_sessions", LongType(), True),
        StructField("video_event_count", LongType(), True),
        StructField("pdf_event_count", LongType(), True),
        StructField("performance_event_count", LongType(), True),
        StructField("navigation_event_count", LongType(), True),
        StructField("completion_event_count", LongType(), True),
        StructField("avg_event_hour", DoubleType(), True),
        StructField("distinct_block_types", LongType(), True),
        StructField("distinct_blocks", LongType(), True),
        StructField("completion_value_sum", DoubleType(), True),
        StructField("first_time", TimestampType(), True),
        StructField("last_time", TimestampType(), True),
        StructField("last_event_time", TimestampType(), True),
        StructField("event_span_minutes", DoubleType(), True),
        StructField("video_share", DoubleType(), True),
        StructField("pdf_share", DoubleType(), True),
        StructField("performance_share", DoubleType(), True),
        StructField("navigation_share", DoubleType(), True),
    ]
)
