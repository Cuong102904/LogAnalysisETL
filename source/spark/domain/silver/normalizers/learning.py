from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_learning(df: DataFrame) -> DataFrame:
    base = df.filter(F.col("silver_class") == "learning")
    return base.select(
        F.to_timestamp(F.get_json_object("value_raw", "$.time")).alias("ts"),
        F.to_date(F.to_timestamp(F.get_json_object("value_raw", "$.time"))).alias("event_date"),
        F.get_json_object("value_raw", "$.context.course_id").alias("course_id"),
        F.get_json_object("value_raw", "$.context.org_id").alias("org_id"),
        F.get_json_object("value_raw", "$.context.user_id").cast("long").alias("user_id_int"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        F.get_json_object("value_raw", "$.session").alias("session"),
        F.get_json_object("value_raw", "$.event_source").alias("event_source"),
        F.get_json_object("value_raw", "$.name").alias("name"),
        F.get_json_object("value_raw", "$.event_type").alias("event_type"),
        F.get_json_object("value_raw", "$.context.path").alias("path"),
        F.get_json_object("value_raw", "$.referer").alias("referer"),
        F.get_json_object("value_raw", "$.page").alias("page"),
        F.get_json_object("value_raw", "$.host").alias("host"),
        F.get_json_object("value_raw", "$.event").alias("event_json"),
        F.get_json_object("value_raw", "$.ip").alias("ip"),
        F.get_json_object("value_raw", "$.agent").alias("agent"),
    )
