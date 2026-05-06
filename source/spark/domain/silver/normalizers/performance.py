from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_performance(df: DataFrame) -> DataFrame:
    base = df.filter(F.col("silver_class") == "performance")
    event_map = F.from_json(F.get_json_object("value_raw", "$.event"), "map<string,string>")
    return base.select(
        F.to_timestamp(F.get_json_object("value_raw", "$.time")).alias("ts"),
        F.to_date(F.to_timestamp(F.get_json_object("value_raw", "$.time"))).alias("event_date"),
        F.get_json_object("value_raw", "$.context.course_id").alias("course_id"),
        F.get_json_object("value_raw", "$.context.user_id").cast("long").alias("user_id_int"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        event_map.getItem("problem_id").alias("problem_id"),
        event_map.getItem("event_transaction_id").alias("event_transaction_id"),
    )
