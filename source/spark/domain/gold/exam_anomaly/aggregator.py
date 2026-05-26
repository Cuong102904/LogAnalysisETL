from pyspark.sql import DataFrame, functions as F

from domain.gold.common import EXAM_STATE_WATERMARK, filter_exam_security_events, rolling_anomaly_score


def build_exam_anomaly_features(exam_df: DataFrame, system_df: DataFrame) -> DataFrame:
    exam_base = (
        exam_df.withWatermark("time", EXAM_STATE_WATERMARK).withColumn(
            "attempt_event", F.lower(F.col("attempt_event"))
        )
    )
    attempt_keys = [
        "event_date",
        "course_id",
        "exam_id",
        "exam_content_id",
        "exam_name",
        "attempt_id",
        "attempt_user_id",
        "user_id",
        "username",
        "time_limit_mins",
        "allowed_time_limit_mins",
        "exam_is_proctored",
        "exam_is_practice_exam",
        "exam_is_active",
    ]
    attempt_grouped = exam_base.groupBy(*attempt_keys).agg(
        F.count("*").alias("event_count"),
        F.approx_count_distinct("session_id").alias("distinct_sessions"),
        F.approx_count_distinct("ip").alias("distinct_exam_ips"),
        F.sum((F.col("attempt_event") == "created").cast("int")).alias("created_count"),
        F.sum((F.col("attempt_event") == "started").cast("int")).alias("started_count"),
        F.sum((F.col("attempt_event") == "ready_to_submit").cast("int")).alias(
            "ready_to_submit_count"
        ),
        F.sum((F.col("attempt_event") == "submitted").cast("int")).alias("submitted_count"),
        F.max("elapsed_time_secs").alias("max_elapsed_time_secs"),
        F.min("attempt_started_at").alias("attempt_started_at"),
        F.max("attempt_completed_at").alias("attempt_completed_at"),
        F.min("time").alias("first_time"),
        F.max("time").alias("last_time"),
        F.max("time").alias("last_event_time"),
    ).withColumn(
        "attempt_window_start",
        F.coalesce(F.col("attempt_started_at"), F.col("first_time")),
    ).withColumn(
        "attempt_window_end",
        F.coalesce(F.col("attempt_completed_at"), F.col("last_event_time")),
    ).withColumn(
        "attempt_duration_secs",
        F.when(
            F.col("attempt_window_start").isNotNull() & F.col("attempt_window_end").isNotNull(),
            F.unix_timestamp("attempt_window_end") - F.unix_timestamp("attempt_window_start"),
        ).otherwise(F.lit(None).cast("double")),
    )

    attempt_scored = rolling_anomaly_score(
        attempt_grouped,
        ["course_id", "exam_id"],
        "event_count",
        ["event_date", "attempt_window_start", "attempt_id"],
    ).withColumn("anomaly_domain", F.lit("exam")).withColumn(
        "anomaly_type", F.lit("attempt_activity")
    ).withColumn(
        "entity_type", F.lit("exam_attempt")
    ).withColumn(
        "entity_id",
        F.concat_ws("|", F.col("exam_id").cast("string"), F.col("attempt_id").cast("string")),
    ).withColumn(
        "metric_name", F.lit("event_count")
    ).withColumn(
        "metric_value", F.col("event_count").cast("double")
    ).withColumn(
        "is_anomaly", F.col("is_anomaly") & (F.col("submitted_count") >= F.lit(1))
    )

    security_events = filter_exam_security_events(
        system_df.withWatermark("time", EXAM_STATE_WATERMARK)
    ).select(
        "course_id",
        "user_id",
        "session_id",
        "ip",
        "time",
        "event_type",
    )
    security_joined = (
        attempt_grouped.select(
            *[F.col(k).alias(k) for k in attempt_keys],
            F.col("attempt_started_at"),
            F.col("attempt_completed_at"),
            F.col("attempt_window_start"),
            F.col("attempt_window_end"),
        )
        .alias("a")
        .join(
            security_events.alias("s"),
            (F.col("a.user_id") == F.col("s.user_id"))
            & (
                F.col("s.course_id").isNull()
                | (F.col("a.course_id") == F.col("s.course_id"))
            )
            & F.col("s.time").between(F.col("a.attempt_window_start"), F.col("a.attempt_window_end")),
            "left",
        )
        .select(
            *[F.col(f"a.{k}").alias(k) for k in attempt_keys],
            F.col("a.attempt_started_at").alias("attempt_started_at"),
            F.col("a.attempt_completed_at").alias("attempt_completed_at"),
            F.col("a.attempt_window_start").alias("attempt_window_start"),
            F.col("a.attempt_window_end").alias("attempt_window_end"),
            F.col("s.session_id").alias("security_session_id"),
            F.col("s.ip").alias("security_ip"),
            F.col("s.time").alias("security_time"),
            F.col("s.event_type").alias("security_event_type"),
        )
    )
    security_grouped = security_joined.groupBy(
        *attempt_keys,
        "attempt_started_at",
        "attempt_completed_at",
        "attempt_window_start",
        "attempt_window_end",
    ).agg(
        F.count("security_ip").alias("security_event_count"),
        F.approx_count_distinct("security_session_id").alias("distinct_security_sessions"),
        F.approx_count_distinct("security_ip").alias("distinct_login_ips"),
        F.sum(
            F.when(F.col("security_event_type").contains("login"), F.lit(1)).otherwise(F.lit(0))
        ).alias(
            "login_event_count"
        ),
        F.sum(
            F.when(F.col("security_event_type").contains("proctoring"), F.lit(1)).otherwise(F.lit(0))
        ).alias(
            "proctoring_event_count"
        ),
        F.min("security_time").alias("first_security_time"),
        F.max("security_time").alias("last_security_time"),
    ).withColumn(
        "first_time",
        F.coalesce(F.col("first_security_time"), F.col("attempt_window_start")),
    ).withColumn(
        "last_time",
        F.coalesce(F.col("last_security_time"), F.col("attempt_window_end")),
    ).withColumn(
        "last_event_time",
        F.coalesce(F.col("last_security_time"), F.col("attempt_window_end")),
    ).drop(
        "first_security_time",
        "last_security_time",
    )

    security_scored = rolling_anomaly_score(
        security_grouped,
        ["course_id", "exam_id"],
        "distinct_login_ips",
        ["event_date", "attempt_window_start", "attempt_id"],
    ).withColumn("anomaly_domain", F.lit("exam")).withColumn(
        "anomaly_type", F.lit("multi_ip_login")
    ).withColumn(
        "entity_type", F.lit("exam_attempt")
    ).withColumn(
        "entity_id",
        F.concat_ws("|", F.col("exam_id").cast("string"), F.col("attempt_id").cast("string")),
    ).withColumn(
        "metric_name", F.lit("distinct_login_ips")
    ).withColumn(
        "metric_value", F.col("distinct_login_ips").cast("double")
    ).withColumn(
        "is_anomaly", F.col("distinct_login_ips") >= F.lit(2)
    )
    return attempt_scored.unionByName(security_scored, allowMissingColumns=True)
