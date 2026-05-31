from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _severity_from_zscore(z_score: F.Column) -> F.Column:
    return (
        F.when(z_score >= F.lit(4.0), F.lit("critical"))
        .when(z_score >= F.lit(3.0), F.lit("high"))
        .when(z_score >= F.lit(2.0), F.lit("medium"))
        .otherwise(F.lit("low"))
    )


def build_alert_events(
    behavior_anomaly_signals_df: DataFrame,
    min_event_count: int = 3,
    min_distinct_users: int = 2,
    min_z_score: float = 2.0,
) -> DataFrame:
    filtered = behavior_anomaly_signals_df.filter(
        F.col("is_anomaly")
        & (F.col("event_count") >= F.lit(min_event_count))
        & (F.col("distinct_users") >= F.lit(min_distinct_users))
        & (F.coalesce(F.col("z_score"), F.lit(0.0)) >= F.lit(min_z_score))
    )

    alert_time = F.coalesce(F.col("last_time"), F.col("last_event_time"))
    alert_type = F.concat_ws(
        "_",
        F.col("alert_domain"),
        F.lit("anomaly"),
    )
    alert_message = F.concat(
        F.lit("Anomaly detected for "),
        F.col("alert_domain"),
        F.lit(" "),
        F.col("entity_type"),
        F.lit(" "),
        F.col("entity_id"),
        F.lit(" on course "),
        F.coalesce(F.col("course_id"), F.lit("unknown")),
        F.lit(": metric "),
        F.col("metric_name"),
        F.lit("="),
        F.col("metric_value").cast("string"),
        F.lit(", z="),
        F.round(F.coalesce(F.col("z_score"), F.lit(0.0)), 2).cast("string"),
    )

    return (
        filtered.withColumn(
            "alert_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("anomaly_domain"),
                    F.col("entity_type"),
                    F.col("entity_id"),
                    F.col("event_date").cast("string"),
                    F.coalesce(F.col("course_id"), F.lit("unknown")),
                    F.col("metric_name"),
                    F.col("metric_value").cast("string"),
                    F.col("z_score").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("alert_domain", F.col("anomaly_domain"))
        .withColumn(
            "alert_severity", _severity_from_zscore(F.coalesce(F.col("z_score"), F.lit(0.0)))
        )
        .withColumn("alert_type", alert_type)
        .withColumn("alert_message", alert_message)
        .withColumn("alert_time", alert_time)
        .select(
            "alert_id",
            "alert_domain",
            "entity_type",
            "entity_id",
            "event_date",
            "course_id",
            "metric_name",
            "metric_value",
            "event_count",
            "distinct_users",
            "distinct_sessions",
            "first_time",
            "last_time",
            "last_event_time",
            "rolling_mean_7",
            "rolling_std_7",
            "z_score",
            "is_anomaly",
            "alert_severity",
            "alert_type",
            "alert_message",
            "alert_time",
        )
    )
