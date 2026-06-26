from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects.daotao_ai.gold.domain.common import ensure_event_date
from projects.daotao_ai.gold.schemas.pdf_behavior_features import (
    PDF_ENGAGEMENT_FEATURES_SCHEMA,
)


def build_pdf_behavior_features(pdf_df: DataFrame) -> DataFrame:
    document_action = F.lower(F.coalesce(F.col("document_action"), F.lit("")))
    base = (
        pdf_df.withColumn("user_id", F.col("actor_id").cast("long"))
        .withColumn("time", F.col("event_time"))
        .withColumn(
            "content_id",
            F.coalesce(
                F.col("file_name"),
                F.col("asset_url"),
                F.col("document_id"),
                F.col("chapter"),
                F.col("chapter_title"),
            ),
        )
        .withColumn("is_scroll", document_action == F.lit("scroll"))
        .withColumn("is_zoom", document_action == F.lit("zoom"))
        .withColumn("is_zoom_button_change", F.lit(False))
        .withColumn(
            "scroll_up",
            F.when(F.col("scroll_direction") == "up", F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "scroll_down",
            F.when(F.col("scroll_direction") == "down", F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn("page_number", F.col("page_number").cast("int"))
    )
    base = ensure_event_date(base)
    return (
        base.groupBy("event_date", "course_id", "user_id", "content_id", "chapter")
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("session_id").alias("distinct_sessions"),
            F.sum(F.col("is_scroll").cast("int")).alias("scroll_count"),
            F.sum((F.col("is_zoom") | F.col("is_zoom_button_change")).cast("int")).alias(
                "zoom_count"
            ),
            F.sum(F.col("scroll_up")).alias("scroll_up_count"),
            F.sum(F.col("scroll_down")).alias("scroll_down_count"),
            F.approx_count_distinct("page_number").alias("distinct_pages"),
            F.avg("page_number").alias("avg_page_number"),
            F.max("page_number").alias("max_page_number"),
            F.min("page_number").alias("min_page_number"),
            F.avg("zoom_amount").alias("avg_scale_amount"),
            F.min("time").alias("first_time"),
            F.max("time").alias("last_time"),
            F.max("time").alias("last_event_time"),
        )
        .withColumn(
            "scroll_balance",
            F.col("scroll_down_count") - F.col("scroll_up_count"),
        )
        .select(*[field.name for field in PDF_ENGAGEMENT_FEATURES_SCHEMA])
    )
