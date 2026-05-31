from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

QUIZ_ATTEMPT_METRICS_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), True),
        StructField("course_id", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("problem_id", StringType(), True),
        StructField("problem_type", StringType(), True),
        StructField("event_count", LongType(), True),
        StructField("distinct_sessions", LongType(), True),
        StructField("submit_count", LongType(), True),
        StructField("check_count", LongType(), True),
        StructField("graded_count", LongType(), True),
        StructField("avg_weighted_earned", DoubleType(), True),
        StructField("avg_weighted_possible", DoubleType(), True),
        StructField("avg_score_ratio", DoubleType(), True),
        StructField("max_score_ratio", DoubleType(), True),
        StructField("min_score_ratio", DoubleType(), True),
        StructField("first_time", TimestampType(), True),
        StructField("last_time", TimestampType(), True),
        StructField("last_event_time", TimestampType(), True),
        StructField("attempt_count", LongType(), True),
    ]
)
