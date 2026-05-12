from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_learning(df: DataFrame) -> DataFrame:
    """
    Base table for all authenticated user events.
    Normalizes common metadata fields present in every raw log record.

    This table is the catch-all for every event with a username.
    Domain-specific tables (video, performance, navigation, pdf, exam)
    are enrichment tables with specialized schemas — Gold joins them.
    """
    base = df.filter(F.col("silver_class") != "system")
    base = base.filter(
        F.get_json_object("value_raw", "$.username").isNotNull()
        & (F.get_json_object("value_raw", "$.username") != "")
    )

    # Parse block_type and block_id from URL path when present
    # e.g. /courses/.../xblock/block-v1:...+type@video+block@abc123/handler/...
    path_col = F.get_json_object("value_raw", "$.context.path")
    block_type = F.regexp_extract(path_col, r"\+type@([^+]+)\+block@", 1)
    block_id = F.regexp_extract(path_col, r"\+block@([a-f0-9]+)", 1)

    # completion_value from publish_completion POST body (raw payload field)
    event_type_col = F.get_json_object("value_raw", "$.event_type")
    completion_value = F.when(
        event_type_col.contains("publish_completion"),
        F.get_json_object(F.get_json_object("value_raw", "$.event"), "$.completion").cast("int"),
    ).otherwise(F.lit(None).cast("int"))

    return base.select(
        F.col("dedup_key").alias("event_id"),
        F.to_timestamp(F.get_json_object("value_raw", "$.time")).alias("ts"),
        F.to_date(F.to_timestamp(F.get_json_object("value_raw", "$.time"))).alias("event_date"),
        F.hour(F.to_timestamp(F.get_json_object("value_raw", "$.time"))).alias("event_hour"),
        F.get_json_object("value_raw", "$.event_type").alias("event_type"),
        F.get_json_object("value_raw", "$.event_source").alias("event_source"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        F.get_json_object("value_raw", "$.context.user_id").cast("long").alias("user_id"),
        F.get_json_object("value_raw", "$.session").alias("session_id"),
        F.get_json_object("value_raw", "$.context.course_id").alias("course_id"),
        F.get_json_object("value_raw", "$.context.org_id").alias("org_id"),
        # block context (extracted from raw URL, not derived)
        F.when(block_type != "", block_type).otherwise(F.lit(None)).alias("block_type"),
        F.when(block_id != "", block_id).otherwise(F.lit(None)).alias("block_id"),
        # completion signal from raw POST payload
        completion_value.alias("completion_value"),
        # request metadata
        F.get_json_object("value_raw", "$.ip").alias("ip"),
        F.get_json_object("value_raw", "$.host").alias("host"),
        F.get_json_object("value_raw", "$.agent").alias("agent"),
        F.get_json_object("value_raw", "$.referer").alias("referer"),
        F.get_json_object("value_raw", "$.page").alias("page"),
        # raw event payload (for domain-specific downstream processing)
        F.get_json_object("value_raw", "$.event").alias("event_json"),
    )
