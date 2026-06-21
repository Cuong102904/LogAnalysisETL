from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_pdf_behavior_features(pdf_df: DataFrame) -> DataFrame:
    base = (
        pdf_df.withColumn(
            "content_id",
            F.coalesce(F.col("pdf_name"), F.col("doc_url"), F.col("chapter")),
        )
        .withColumn("is_scroll", F.col("event_type").startswith("textbook.pdf.page.scrolled"))
        .withColumn("is_zoom", F.col("event_type").startswith("textbook.pdf.display.scaled"))
        .withColumn(
            "is_zoom_button_change",
            F.col("event_type").startswith("textbook.pdf.zoom.buttons.changed"),
        )
        .withColumn(
            "scroll_up",
            F.when(F.col("scroll_direction") == "up", F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "scroll_down",
            F.when(F.col("scroll_direction") == "down", F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn("page_number", F.col("page_number").cast("int"))
        .withColumn("scale_amount", F.col("scale_amount").cast("double"))
    )
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
            F.avg("scale_amount").alias("avg_scale_amount"),
            F.min("time").alias("first_time"),
            F.max("time").alias("last_time"),
            F.max("time").alias("last_event_time"),
        )
        .withColumn(
            "scroll_balance",
            F.col("scroll_down_count") - F.col("scroll_up_count"),
        )
    )
