from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_navigation(df: DataFrame) -> DataFrame:
    """
    Normalize navigation events: seq_goto, seq_next, seq_prev,
    server-side page views (URL paths), and page_close.
    """
    base = df.filter(F.col("silver_class") == "navigation")
    event_json = F.get_json_object("value_raw", "$.event")
    ev = F.from_json(event_json, "map<string,string>")

    return base.select(
        F.col("dedup_key").alias("event_id"),
        F.col("time").alias("time"),
        F.to_date(F.col("time")).alias("event_date"),
        F.get_json_object("value_raw", "$.event_type").alias("event_type"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        F.get_json_object("value_raw", "$.context.user_id").cast("long").alias("user_id"),
        F.get_json_object("value_raw", "$.session").alias("session_id"),
        F.get_json_object("value_raw", "$.context.course_id").alias("course_id"),
        F.get_json_object("value_raw", "$.context.org_id").alias("org_id"),
        # seq_* event fields
        ev.getItem("id").alias("sequence_id"),
        ev.getItem("current_tab").cast("int").alias("from_tab"),
        ev.getItem("target_tab").cast("int").alias("to_tab"),
        ev.getItem("tab_count").cast("int").alias("tab_count"),
        ev.getItem("old").alias("old_block_id"),
        ev.getItem("new").alias("new_block_id"),
        # page view / navigation context
        F.get_json_object("value_raw", "$.page").alias("page_url"),
        F.get_json_object("value_raw", "$.referer").alias("referer"),
        F.get_json_object("value_raw", "$.event_source").alias("event_source"),
        F.get_json_object("value_raw", "$.host").alias("host"),
    )
