from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_video_interactions(learning_df: DataFrame) -> DataFrame:
    event_map = F.from_json(F.col("event_json"), "map<string,string>")
    action = F.regexp_replace(F.col("event_type"), "_video", "")
    return learning_df.filter(F.col("event_type").contains("video")).select(
        F.col("ts"),
        F.col("event_date"),
        F.col("course_id"),
        F.col("user_id_int"),
        F.col("username"),
        F.col("session"),
        action.alias("action_type"),
        event_map.getItem("id").alias("video_id"),
        event_map.getItem("code").alias("video_code"),
        event_map.getItem("duration").cast("double").alias("video_duration_s"),
        event_map.getItem("currentTime").cast("double").alias("current_time_s"),
    )
