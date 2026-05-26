from pyspark.sql.types import DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

PDF_ENGAGEMENT_FEATURES_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), True),
        StructField("course_id", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("content_id", StringType(), True),
        StructField("chapter", StringType(), True),
        StructField("event_count", LongType(), True),
        StructField("distinct_sessions", LongType(), True),
        StructField("scroll_count", LongType(), True),
        StructField("zoom_count", LongType(), True),
        StructField("scroll_up_count", LongType(), True),
        StructField("scroll_down_count", LongType(), True),
        StructField("distinct_pages", LongType(), True),
        StructField("avg_page_number", DoubleType(), True),
        StructField("max_page_number", IntegerType(), True),
        StructField("min_page_number", IntegerType(), True),
        StructField("avg_scale_amount", DoubleType(), True),
        StructField("scroll_balance", LongType(), True),
        StructField("first_time", TimestampType(), True),
        StructField("last_time", TimestampType(), True),
        StructField("last_event_time", TimestampType(), True),
    ]
)
