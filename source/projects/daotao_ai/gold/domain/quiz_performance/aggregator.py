from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects.daotao_ai.gold.domain.common import safe_ratio


def build_quiz_performance_features(performance_df: DataFrame) -> DataFrame:
    base = (
        performance_df.withColumn("is_submit", F.lower(F.col("event_type")).contains("submit"))
        .withColumn("is_check", F.lower(F.col("event_type")).contains("check"))
        .withColumn("is_graded", F.lower(F.col("event_type")).contains("graded"))
        .withColumn(
            "score_ratio",
            safe_ratio(
                F.col("weighted_earned").cast("double"), F.col("weighted_possible").cast("double")
            ),
        )
    )
    return (
        base.groupBy("event_date", "course_id", "user_id", "problem_id", "problem_type")
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("session_id").alias("distinct_sessions"),
            F.sum(F.col("is_submit").cast("int")).alias("submit_count"),
            F.sum(F.col("is_check").cast("int")).alias("check_count"),
            F.sum(F.col("is_graded").cast("int")).alias("graded_count"),
            F.avg("weighted_earned").alias("avg_weighted_earned"),
            F.avg("weighted_possible").alias("avg_weighted_possible"),
            F.avg("score_ratio").alias("avg_score_ratio"),
            F.max("score_ratio").alias("max_score_ratio"),
            F.min("score_ratio").alias("min_score_ratio"),
            F.min("time").alias("first_time"),
            F.max("time").alias("last_time"),
            F.max("time").alias("last_event_time"),
        )
        .withColumn(
            "attempt_count",
            F.col("submit_count") + F.col("check_count"),
        )
    )
