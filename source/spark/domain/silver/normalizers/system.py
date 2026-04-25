from pyspark.sql import DataFrame, functions as F


def normalize_system(df: DataFrame) -> DataFrame:
    base = df.filter(F.col("silver_class") == "system")
    event_type = F.lower(F.get_json_object("value_raw", "$.event_type"))
    return base.select(
        F.to_timestamp(F.get_json_object("value_raw", "$.time")).alias("ts"),
        F.to_date(F.to_timestamp(F.get_json_object("value_raw", "$.time"))).alias("event_date"),
        F.get_json_object("value_raw", "$.context.user_id").cast("long").alias("user_id_int"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        F.get_json_object("value_raw", "$.session").alias("session"),
        F.get_json_object("value_raw", "$.ip").alias("ip"),
        F.get_json_object("value_raw", "$.agent").alias("agent"),
        event_type.alias("event_type"),
        F.when(event_type.contains("proctor"), F.lit("proctoring"))
        .when(event_type.contains("heartbeat"), F.lit("heartbeat"))
        .when(event_type.contains("auth"), F.lit("auth"))
        .when(event_type.contains("login"), F.lit("login"))
        .otherwise(F.lit("session"))
        .alias("event_category"),
    )
