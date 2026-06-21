from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

VIDEO_ACTIONS = (
    "play_video",
    "pause_video",
    "seek_video",
    "stop_video",
    "speed_change_video",
    "load_video",
)

PDF_ACTION_PREFIXES = (
    "textbook.pdf.page.scrolled",
    "textbook.pdf.display.scaled",
    "textbook.pdf.zoom.buttons.changed",
)

PERFORMANCE_ACTIONS = (
    "edx.grades.problem.submitted",
    "problem_check",
    "problem_graded",
    "problem_save",
    "problem_show",
    "showanswer",
)

EXAM_STATE_WATERMARK = "1 day"


def safe_ratio(numerator: F.Column, denominator: F.Column) -> F.Column:
    return F.when(denominator.isNotNull() & (denominator != 0), numerator / denominator).otherwise(
        F.lit(None).cast("double")
    )


def rolling_anomaly_score(
    df: DataFrame,
    partition_cols: list[str],
    metric_col: str,
    order_cols: list[str] | None = None,
) -> DataFrame:
    window = (
        Window.partitionBy(*partition_cols)
        .orderBy(*(order_cols or ["event_date"]))
        .rowsBetween(-3, 3)
    )
    scored = (
        df.withColumn("rolling_mean_7", F.avg(metric_col).over(window))
        .withColumn("rolling_std_7", F.stddev(metric_col).over(window))
        .withColumn(
            "z_score",
            F.when(
                F.col("rolling_std_7").isNotNull() & (F.col("rolling_std_7") > 0),
                (F.col(metric_col) - F.col("rolling_mean_7")) / F.col("rolling_std_7"),
            ).otherwise(F.lit(0.0)),
        )
    )
    return scored.withColumn(
        "is_anomaly",
        (
            F.col(metric_col)
            >= F.col("rolling_mean_7") + F.coalesce(F.col("rolling_std_7"), F.lit(0.0))
        )
        & (F.col("z_score") >= F.lit(1.5)),
    )


def score_anomaly(
    df: DataFrame,
    anomaly_domain: str,
    entity_type: str,
    entity_id_expr: F.Column,
    partition_cols: list[str],
    extra_group_cols: list[str],
    order_cols: list[str] | None = None,
) -> DataFrame:
    grouped = df.groupBy(*extra_group_cols).agg(
        F.count("*").alias("event_count"),
        F.approx_count_distinct("user_id").alias("distinct_users"),
        F.approx_count_distinct("session_id").alias("distinct_sessions"),
        F.min("time").alias("first_time"),
        F.max("time").alias("last_time"),
        F.max("time").alias("last_event_time"),
    )
    scored = rolling_anomaly_score(grouped, partition_cols, "event_count", order_cols)
    return (
        scored.withColumn("anomaly_domain", F.lit(anomaly_domain))
        .withColumn("entity_type", F.lit(entity_type))
        .withColumn("entity_id", entity_id_expr)
        .withColumn("metric_name", F.lit("event_count"))
        .withColumn("metric_value", F.col("event_count").cast("double"))
        .withColumn(
            "is_anomaly",
            F.col("is_anomaly")
            & (F.col("distinct_users") >= F.lit(2))
            & (F.col("event_count") >= F.lit(3)),
        )
    )


def filter_exam_security_events(system_df: DataFrame) -> DataFrame:
    event_type = F.lower(F.coalesce(F.col("event_type"), F.lit("")))
    path = F.lower(F.coalesce(F.col("path"), F.lit("")))
    return system_df.filter(
        F.col("ip").isNotNull()
        & (
            event_type.contains("login")
            | event_type.contains("logout")
            | event_type.contains("proctoring")
            | event_type.contains("proctored_exam")
            | event_type.startswith("/api/edx_proctoring/")
            | path.contains("/login")
            | path.contains("/logout")
        )
    )
