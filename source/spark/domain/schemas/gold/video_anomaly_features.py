from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

VIDEO_FRICTION_SIGNALS_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), True),
        StructField("course_id", StringType(), True),
        StructField("video_id", StringType(), True),
        StructField("time_bucket_s", IntegerType(), True),
        StructField("wallclock_bucket_ts", TimestampType(), True),
        StructField("action_type", StringType(), True),
        StructField("event_count", LongType(), True),
        StructField("distinct_users", LongType(), True),
        StructField("distinct_sessions", LongType(), True),
        StructField("avg_current_time_s", DoubleType(), True),
        StructField("max_current_time_s", DoubleType(), True),
        StructField("avg_video_duration_s", DoubleType(), True),
        StructField("avg_seek_distance_s", DoubleType(), True),
        StructField("avg_watch_ratio", DoubleType(), True),
        StructField("first_time", TimestampType(), True),
        StructField("last_time", TimestampType(), True),
        StructField("last_event_time", TimestampType(), True),
        StructField("rolling_mean_7", DoubleType(), True),
        StructField("rolling_std_7", DoubleType(), True),
        StructField("z_score", DoubleType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("anomaly_domain", StringType(), True),
        StructField("entity_type", StringType(), True),
        StructField("entity_id", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("metric_value", DoubleType(), True),
    ]
)
