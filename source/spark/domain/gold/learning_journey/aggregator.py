from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from domain.gold.common import PERFORMANCE_ACTIONS, VIDEO_ACTIONS, safe_ratio


def build_learning_journey_features(learning_df: DataFrame) -> DataFrame:
    event_type = F.lower(F.col("event_type"))
    base = (
        learning_df.withColumn(
            "event_category",
            F.when(
                event_type.isin(*VIDEO_ACTIONS) | event_type.contains("save_user_state"),
                F.lit("video"),
            )
            .when(
                event_type.startswith("textbook.pdf.")
                | (event_type == "book")
                | (event_type == "edx.googlecomponent.document.displayed"),
                F.lit("pdf"),
            )
            .when(
                event_type.isin(*PERFORMANCE_ACTIONS)
                | event_type.contains("edx.courseware.index.report")
                | event_type.contains("input_ajax")
                | event_type.contains("create_submission")
                | event_type.contains("student_submit"),
                F.lit("performance"),
            )
            .when(
                event_type.isin(
                    "seq_goto",
                    "seq_next",
                    "seq_prev",
                    "page_close",
                    "edx.ui.lms.sequence.next_selected",
                    "edx.ui.lms.sequence.previous_selected",
                )
                | event_type.contains("edx.courseware.index.access")
                | event_type.contains("jump_to"),
                F.lit("navigation"),
            )
            .otherwise(F.lit("other")),
        )
        .withColumn("has_block", F.col("block_id").isNotNull())
        .withColumn(
            "is_completion",
            F.col("completion_value").isNotNull() & (F.col("completion_value").cast("double") > 0),
        )
    )
    grouped = (
        base.groupBy("event_date", "course_id", "user_id")
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("session_id").alias("distinct_sessions"),
            F.sum((F.col("event_category") == "video").cast("int")).alias("video_event_count"),
            F.sum((F.col("event_category") == "pdf").cast("int")).alias("pdf_event_count"),
            F.sum((F.col("event_category") == "performance").cast("int")).alias(
                "performance_event_count"
            ),
            F.sum((F.col("event_category") == "navigation").cast("int")).alias(
                "navigation_event_count"
            ),
            F.sum(F.col("is_completion").cast("int")).alias("completion_event_count"),
            F.avg("event_hour").alias("avg_event_hour"),
            F.approx_count_distinct(
                F.when(F.col("block_type").isNotNull(), F.col("block_type"))
            ).alias("distinct_block_types"),
            F.approx_count_distinct(F.when(F.col("block_id").isNotNull(), F.col("block_id"))).alias(
                "distinct_blocks"
            ),
            F.sum(F.col("completion_value").cast("double")).alias("completion_value_sum"),
            F.min("time").alias("first_time"),
            F.max("time").alias("last_time"),
            F.max("time").alias("last_event_time"),
        )
        .withColumn(
            "event_span_minutes",
            (F.unix_timestamp("last_time") - F.unix_timestamp("first_time")) / 60.0,
        )
    )
    return (
        grouped.withColumn(
            "video_share", safe_ratio(F.col("video_event_count"), F.col("event_count"))
        )
        .withColumn("pdf_share", safe_ratio(F.col("pdf_event_count"), F.col("event_count")))
        .withColumn(
            "performance_share",
            safe_ratio(F.col("performance_event_count"), F.col("event_count")),
        )
        .withColumn(
            "navigation_share",
            safe_ratio(F.col("navigation_event_count"), F.col("event_count")),
        )
    )
