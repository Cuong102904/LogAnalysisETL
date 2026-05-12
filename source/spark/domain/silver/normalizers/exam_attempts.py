from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_exam_attempts(df: DataFrame) -> DataFrame:
    """
    Normalize timed/proctored exam attempt state transitions.
    Sources: edx.special_exam.timed.attempt.{created,started,ready_to_submit,submitted}

    Each record = one state transition of an attempt. Gold will aggregate per
    (user, exam) to build attempt profiles for anomaly/fraud detection.

    All fields come directly from the raw event payload — no derivation at Silver.
    """
    base = df.filter(F.col("silver_class") == "exam")
    ev = F.from_json(F.get_json_object("value_raw", "$.event"), "map<string,string>")

    return base.select(
        F.col("dedup_key").alias("event_id"),
        F.to_timestamp(F.get_json_object("value_raw", "$.time")).alias("ts"),
        F.to_date(F.to_timestamp(F.get_json_object("value_raw", "$.time"))).alias(
            "event_date"
        ),
        # State transition type: created | started | ready_to_submit | submitted
        F.regexp_extract(
            F.get_json_object("value_raw", "$.event_type"),
            r"edx\.special_exam\.timed\.attempt\.(.+)$",
            1,
        ).alias("attempt_event"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        F.get_json_object("value_raw", "$.context.user_id")
        .cast("long")
        .alias("user_id"),
        F.get_json_object("value_raw", "$.session").alias("session_id"),
        F.get_json_object("value_raw", "$.context.course_id").alias("course_id"),
        # Attempt fields — direct from raw payload
        ev.getItem("attempt_id").cast("long").alias("attempt_id"),
        ev.getItem("attempt_user_id").cast("long").alias("attempt_user_id"),
        ev.getItem("exam_id").cast("long").alias("exam_id"),
        ev.getItem("exam_content_id").alias("exam_content_id"),
        ev.getItem("exam_name").alias("exam_name"),
        ev.getItem("exam_is_proctored").cast("boolean").alias("exam_is_proctored"),
        ev.getItem("exam_is_practice_exam")
        .cast("boolean")
        .alias("exam_is_practice_exam"),
        ev.getItem("exam_is_active").cast("boolean").alias("exam_is_active"),
        ev.getItem("exam_default_time_limit_mins")
        .cast("int")
        .alias("time_limit_mins"),
        ev.getItem("attempt_allowed_time_limit_mins")
        .cast("int")
        .alias("allowed_time_limit_mins"),
        F.to_timestamp(ev.getItem("attempt_started_at")).alias("attempt_started_at"),
        F.to_timestamp(ev.getItem("attempt_completed_at")).alias(
            "attempt_completed_at"
        ),
        ev.getItem("attempt_status").alias("attempt_status"),
        ev.getItem("attempt_event_elapsed_time_secs")
        .cast("int")
        .alias("elapsed_time_secs"),
        ev.getItem("attempt_code").alias("attempt_code"),
        F.col("ingest_ts").alias("ingested_at"),
    )
