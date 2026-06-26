from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

VIDEO_ACTIONS = (
    "interact",
    "save_position",
    "transcript",
)

PDF_ACTION_PREFIXES = (
    "view",
    "scroll",
    "zoom",
    "load",
    "search",
)

PERFORMANCE_ACTIONS = (
    "submit",
    "check",
    "grade",
    "grade_feedback",
    "input_ajax",
)

NAVIGATION_ACTIONS = (
    "navigate",
    "sequence_next",
    "sequence_previous",
    "link_clicked",
    "resume_course",
    "sequence_tab",
    "display",
)

COMPLETION_ACTIONS = (
    "complete",
    "completed",
    "finish",
    "finished",
    "passed",
)

EXAM_STATE_WATERMARK = "1 day"


def ensure_event_date(df: DataFrame, time_column: str = "time") -> DataFrame:
    event_date_expr = F.to_date(F.col(time_column))
    if "event_date" in df.columns:
        event_date_expr = F.coalesce(F.col("event_date"), event_date_expr)
    return df.withColumn("event_date", event_date_expr)


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
    system_action = F.lower(F.coalesce(F.col("system_action"), F.lit("")))
    path = F.lower(F.coalesce(F.col("path"), F.lit("")))
    reason = F.lower(F.coalesce(F.col("reason"), F.lit("")))
    return system_df.filter(
        path.contains("/login")
        | path.contains("/logout")
        | path.contains("/api/edx_proctoring/")
        | system_action.contains("login")
        | system_action.contains("logout")
        | system_action.contains("proctoring")
        | reason.contains("login")
        | reason.contains("logout")
        | reason.contains("proctoring")
    )
