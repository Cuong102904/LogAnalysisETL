from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects.daotao_ai.gold.domain.common import (
    EXAM_STATE_WATERMARK,
    ensure_event_date,
    filter_exam_security_events,
    rolling_anomaly_score,
)
from projects.daotao_ai.gold.schemas.exam_anomaly_features import (
    EXAM_INTEGRITY_SIGNALS_SCHEMA,
)


def _with_exam_common_columns(df: DataFrame) -> DataFrame:
    return ensure_event_date(
        df.withColumn("time", F.col("event_time"))
        .withColumn("user_id", F.col("actor_id").cast("long"))
        .withColumn("username", F.col("actor_id").cast("string"))
    )


def build_exam_anomaly_features(exam_df: DataFrame, system_df: DataFrame) -> DataFrame:
    exam_base = (
        _with_exam_common_columns(exam_df)
        .withColumn("exam_id", F.col("exam_id").cast("long"))
        .withColumn("attempt_id", F.col("attempt_id").cast("long"))
        .withColumn("attempt_user_id", F.col("attempt_user_id").cast("long"))
        .withColumn("exam_default_time_limit_mins", F.col("exam_default_time_limit_mins").cast("long"))
        .withColumn("time_limit_mins", F.col("exam_default_time_limit_mins").cast("long"))
        .withColumn("allowed_time_limit_mins", F.col("exam_default_time_limit_mins").cast("long"))
        .withColumn("exam_is_proctored", F.col("exam_is_proctored").cast("boolean"))
        .withColumn("exam_is_practice_exam", F.col("exam_is_practice_exam").cast("boolean"))
        .withColumn("exam_is_active", F.col("exam_is_active").cast("boolean"))
        .withWatermark("time", EXAM_STATE_WATERMARK)
    )
    attempt_state = F.lower(
        F.coalesce(F.col("attempt_status"), F.col("quiz_nav_action"), F.col("exam_action"), F.lit(""))
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

    attempt_grouped = (
        exam_base.withColumn(
            "time_limit_mins", F.col("exam_default_time_limit_mins").cast("long")
        )
        .withColumn("allowed_time_limit_mins", F.col("exam_default_time_limit_mins").cast("long"))
        .groupBy(*attempt_keys)
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("session_id").alias("distinct_sessions"),
            F.lit(0).cast("long").alias("distinct_exam_ips"),
            F.sum(F.when(attempt_state.contains("create"), F.lit(1)).otherwise(F.lit(0))).alias(
                "created_count"
            ),
            F.sum(F.when(attempt_state.contains("start"), F.lit(1)).otherwise(F.lit(0))).alias(
                "started_count"
            ),
            F.sum(F.when(attempt_state.contains("ready"), F.lit(1)).otherwise(F.lit(0))).alias(
                "ready_to_submit_count"
            ),
            F.sum(
                F.when(
                    attempt_state.contains("submit")
                    | attempt_state.contains("complete")
                    | attempt_state.contains("finish"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("submitted_count"),
            F.max(F.col("attempt_elapsed_time_secs").cast("long")).alias("max_elapsed_time_secs"),
            F.min("attempt_started_at").alias("attempt_started_at"),
            F.max("attempt_completed_at").alias("attempt_completed_at"),
            F.min("time").alias("first_time"),
            F.max("time").alias("last_time"),
            F.max("time").alias("last_event_time"),
        )
        .withColumn(
            "attempt_window_start",
            F.coalesce(F.col("attempt_started_at"), F.col("first_time")),
        )
        .withColumn(
            "attempt_window_end",
            F.coalesce(F.col("attempt_completed_at"), F.col("last_event_time")),
        )
        .withColumn(
            "attempt_duration_secs",
            F.when(
                F.col("attempt_window_start").isNotNull() & F.col("attempt_window_end").isNotNull(),
                F.unix_timestamp("attempt_window_end") - F.unix_timestamp("attempt_window_start"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    attempt_scored = (
        rolling_anomaly_score(
            attempt_grouped,
            ["course_id", "exam_id"],
            "event_count",
            ["event_date", "attempt_window_start", "attempt_id"],
        )
        .withColumn("anomaly_domain", F.lit("exam"))
        .withColumn("anomaly_type", F.lit("attempt_activity"))
        .withColumn("entity_type", F.lit("exam_attempt"))
        .withColumn(
            "entity_id",
            F.concat_ws("|", F.col("exam_id").cast("string"), F.col("attempt_id").cast("string")),
        )
        .withColumn("metric_name", F.lit("event_count"))
        .withColumn("metric_value", F.col("event_count").cast("double"))
        .withColumn("is_anomaly", F.col("is_anomaly") & (F.col("submitted_count") >= F.lit(1)))
    )

    proctoring_events = (
        exam_base.filter(F.lower(F.coalesce(F.col("exam_action"), F.lit(""))) == F.lit("proctoring"))
        .select(
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
            "session_id",
            "time",
            F.lit("proctoring").alias("security_event_type"),
        )
    )

    system_security_events = filter_exam_security_events(
        _with_exam_common_columns(system_df).withWatermark("time", EXAM_STATE_WATERMARK)
    ).select(
        F.col("event_date"),
        F.col("course_id"),
        F.lit(None).cast("long").alias("exam_id"),
        F.lit(None).cast("string").alias("exam_content_id"),
        F.lit(None).cast("string").alias("exam_name"),
        F.lit(None).cast("long").alias("attempt_id"),
        F.lit(None).cast("long").alias("attempt_user_id"),
        F.col("user_id"),
        F.col("username"),
        F.lit(None).cast("long").alias("time_limit_mins"),
        F.lit(None).cast("long").alias("allowed_time_limit_mins"),
        F.lit(None).cast("boolean").alias("exam_is_proctored"),
        F.lit(None).cast("boolean").alias("exam_is_practice_exam"),
        F.lit(None).cast("boolean").alias("exam_is_active"),
        F.col("session_id"),
        F.col("time"),
        F.lower(F.coalesce(F.col("system_action"), F.col("reason"), F.lit(""))).alias(
            "security_event_type"
        ),
    )

    security_source = proctoring_events.unionByName(system_security_events, allowMissingColumns=True)
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
            security_source.alias("s"),
            (F.col("a.user_id") == F.col("s.user_id"))
            & (F.col("s.course_id").isNull() | (F.col("a.course_id") == F.col("s.course_id")))
            & F.col("s.time").between(
                F.col("a.attempt_window_start"), F.col("a.attempt_window_end")
            ),
            "left",
        )
        .select(
            *[F.col(f"a.{k}").alias(k) for k in attempt_keys],
            F.col("a.attempt_started_at").alias("attempt_started_at"),
            F.col("a.attempt_completed_at").alias("attempt_completed_at"),
            F.col("a.attempt_window_start").alias("attempt_window_start"),
            F.col("a.attempt_window_end").alias("attempt_window_end"),
            F.col("s.session_id").alias("security_session_id"),
            F.col("s.time").alias("security_time"),
            F.col("s.security_event_type").alias("security_event_type"),
        )
    )

    security_grouped = (
        security_joined.groupBy(
            *attempt_keys,
            "attempt_started_at",
            "attempt_completed_at",
            "attempt_window_start",
            "attempt_window_end",
        )
        .agg(
            F.count("security_session_id").alias("security_event_count"),
            F.approx_count_distinct("security_session_id").alias("distinct_security_sessions"),
            F.lit(0).cast("long").alias("distinct_login_ips"),
            F.sum(
                F.when(F.col("security_event_type").contains("login"), F.lit(1)).otherwise(F.lit(0))
            ).alias("login_event_count"),
            F.sum(
                F.when(
                    F.col("security_event_type").contains("proctoring"), F.lit(1)
                ).otherwise(F.lit(0))
            ).alias("proctoring_event_count"),
            F.min("security_time").alias("first_security_time"),
            F.max("security_time").alias("last_security_time"),
        )
        .withColumn(
            "first_time",
            F.coalesce(F.col("first_security_time"), F.col("attempt_window_start")),
        )
        .withColumn(
            "last_time",
            F.coalesce(F.col("last_security_time"), F.col("attempt_window_end")),
        )
        .withColumn(
            "last_event_time",
            F.coalesce(F.col("last_security_time"), F.col("attempt_window_end")),
        )
        .drop("first_security_time", "last_security_time")
    )

    security_scored = (
        rolling_anomaly_score(
            security_grouped,
            ["course_id", "exam_id"],
            "security_event_count",
            ["event_date", "attempt_window_start", "attempt_id"],
        )
        .withColumn("anomaly_domain", F.lit("exam"))
        .withColumn("anomaly_type", F.lit("security_activity"))
        .withColumn("entity_type", F.lit("exam_attempt"))
        .withColumn(
            "entity_id",
            F.concat_ws("|", F.col("exam_id").cast("string"), F.col("attempt_id").cast("string")),
        )
        .withColumn("metric_name", F.lit("security_event_count"))
        .withColumn("metric_value", F.col("security_event_count").cast("double"))
        .withColumn(
            "is_anomaly",
            F.col("security_event_count") >= F.lit(2),
        )
    )

    return (
        attempt_scored.unionByName(security_scored, allowMissingColumns=True)
        .select(*[field.name for field in EXAM_INTEGRITY_SIGNALS_SCHEMA])
    )
