from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_system(df: DataFrame) -> DataFrame:
    """
    Normalize system/infrastructure events:
    - Unauthenticated requests (bots, crawlers, scanners)
    - Auth/login/logout flows
    - Proctoring API calls
    - Any server-side infra path

    Stores raw agent string for bot/device detection at Gold layer.
    """
    base = df.filter(F.col("silver_class") == "system")
    return base.select(
        F.col("dedup_key").alias("event_id"),
        F.col("time").alias("time"),
        F.to_date(F.col("time")).alias("event_date"),
        F.get_json_object("value_raw", "$.event_type").alias("event_type"),
        F.get_json_object("value_raw", "$.event_source").alias("event_source"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        F.get_json_object("value_raw", "$.context.user_id").cast("long").alias("user_id"),
        F.get_json_object("value_raw", "$.session").alias("session_id"),
        F.get_json_object("value_raw", "$.context.course_id").alias("course_id"),
        F.get_json_object("value_raw", "$.context.org_id").alias("org_id"),
        F.get_json_object("value_raw", "$.ip").alias("ip"),
        F.get_json_object("value_raw", "$.host").alias("host"),
        # Raw agent string — parsed into browser/os/device_type at Gold
        F.get_json_object("value_raw", "$.agent").alias("agent"),
        F.get_json_object("value_raw", "$.referer").alias("referer"),
        F.get_json_object("value_raw", "$.context.path").alias("path"),
        F.get_json_object("value_raw", "$.accept_language").alias("accept_language"),
    )
