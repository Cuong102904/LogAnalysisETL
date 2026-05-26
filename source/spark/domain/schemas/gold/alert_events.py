from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ANOMALY_ALERTS_SCHEMA = StructType(
    [
        StructField("alert_id", StringType(), True),
        StructField("alert_domain", StringType(), True),
        StructField("entity_type", StringType(), True),
        StructField("entity_id", StringType(), True),
        StructField("event_date", DateType(), True),
        StructField("course_id", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("event_count", LongType(), True),
        StructField("distinct_users", LongType(), True),
        StructField("distinct_sessions", LongType(), True),
        StructField("first_time", TimestampType(), True),
        StructField("last_time", TimestampType(), True),
        StructField("last_event_time", TimestampType(), True),
        StructField("rolling_mean_7", DoubleType(), True),
        StructField("rolling_std_7", DoubleType(), True),
        StructField("z_score", DoubleType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("alert_severity", StringType(), True),
        StructField("alert_type", StringType(), True),
        StructField("alert_message", StringType(), True),
        StructField("alert_time", TimestampType(), True),
    ]
)
