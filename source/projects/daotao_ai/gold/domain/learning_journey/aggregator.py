from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects.daotao_ai.gold.domain.common import (
    COMPLETION_ACTIONS,
    NAVIGATION_ACTIONS,
    PDF_ACTION_PREFIXES,
    PERFORMANCE_ACTIONS,
    ensure_event_date,
    VIDEO_ACTIONS,
    safe_ratio,
)
from projects.daotao_ai.gold.schemas.learning_journey_features import (
    USER_LEARNING_PROFILE_DAILY_SCHEMA,
)


def build_learning_journey_features(learning_df: DataFrame) -> DataFrame:
    event_group = F.lower(F.coalesce(F.col("event_group"), F.lit("")))
    normalized_type = F.lower(F.coalesce(F.col("normalized_type"), F.lit("")))
    action = F.lower(F.coalesce(F.col("action"), F.lit("")))
    object_type = F.lower(F.coalesce(F.col("object_type"), F.lit("")))
    base = (
        learning_df.withColumn("user_id", F.col("actor_id").cast("long"))
        .withColumn("time", F.col("event_time"))
        .filter(
            (F.col("learning_relevance") == F.lit("learning"))
            & (F.coalesce(F.col("is_noise"), F.lit(False)) == F.lit(False))
        )
        .withColumn(
            "event_category",
            F.when(
                (
                    event_group.isin("video")
                    | object_type.isin("video")
                    | normalized_type.contains("video")
                )
                & ~event_group.isin("course_content")
                | action.isin(*VIDEO_ACTIONS),
                F.lit("video"),
            )
            .when(
                event_group.isin("document")
                | object_type.isin("document")
                | normalized_type.contains("document")
                | action.isin(*PDF_ACTION_PREFIXES),
                F.lit("pdf"),
            )
            .when(
                event_group.isin("assessment")
                | object_type.isin("problem", "assessment")
                | normalized_type.contains("assessment")
                | action.isin(*PERFORMANCE_ACTIONS),
                F.lit("performance"),
            )
            .when(
                event_group.isin("navigation", "course_content")
                | object_type.isin("page", "course_content")
                | normalized_type.contains("navigation")
                | action.isin(*NAVIGATION_ACTIONS),
                F.lit("navigation"),
            )
            .otherwise(F.lit("other")),
        )
        .withColumn(
            "completion_value",
            F.when(
                normalized_type.contains("completion") | action.isin(*COMPLETION_ACTIONS),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
    )
    base = ensure_event_date(base)
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
            F.sum((F.col("completion_value") > F.lit(0)).cast("int")).alias(
                "completion_event_count"
            ),
            F.avg(F.coalesce(F.col("event_hour").cast("double"), F.hour("time").cast("double"))).alias(
                "avg_event_hour"
            ),
            F.approx_count_distinct(F.when(F.col("object_type").isNotNull(), F.col("object_type"))).alias(
                "distinct_block_types"
            ),
            F.approx_count_distinct(F.when(F.col("object_id").isNotNull(), F.col("object_id"))).alias(
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
        .select(*[field.name for field in USER_LEARNING_PROFILE_DAILY_SCHEMA])
    )
