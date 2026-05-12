from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_video_interactions(learning_df: DataFrame) -> DataFrame:
    """
    Normalize video interaction events from the learning stream.
    Sources: play_video, pause_video, seek_video, stop_video,
             speed_change_video, load_video, save_user_state (video block URL).

    Note: seek_direction (forward/backward) is a derived field -> computed at Gold.
    """
    ev = F.from_json(F.col("event_json"), "map<string,string>")
    video_events = (
        F.col("event_type").isin(
            "play_video", "pause_video", "seek_video", "stop_video",
            "speed_change_video", "load_video",
        )
        | (
            F.col("event_type").contains("save_user_state")
            & F.col("event_type").contains("+type@video+block@")
        )
    )

    return learning_df.filter(video_events).select(
        F.col("event_id"),
        F.col("ts"),
        F.col("event_date"),
        F.col("event_type"),
        F.col("username"),
        F.col("user_id"),
        F.col("session_id"),
        F.col("course_id"),
        F.col("org_id"),
        # video identity: use event payload 'id', fallback to URL 'block_id' for save_user_state
        F.coalesce(ev.getItem("id"), F.col("block_id")).alias("video_id"),
        ev.getItem("code").alias("video_code"),
        ev.getItem("duration").cast("double").alias("video_duration"),
        # position fields (null for events that don't carry them)
        ev.getItem("currentTime").cast("double").alias("current_time"),
        # seek fields (only seek_video)
        ev.getItem("old_time").cast("double").alias("old_time"),
        ev.getItem("new_time").cast("double").alias("new_time"),
        ev.getItem("type").alias("seek_type"),           # 'html5' | 'youtube'
        # speed change fields (only speed_change_video)
        ev.getItem("old_speed").cast("double").alias("old_speed"),
        ev.getItem("new_speed").cast("double").alias("new_speed"),
        # save_user_state POST body field (nested under POST array)
        F.coalesce(
            ev.getItem("saved_video_position"),
            F.get_json_object(F.col("event_json"), "$.POST.saved_video_position[0]")
        ).alias("saved_position"),
    )
