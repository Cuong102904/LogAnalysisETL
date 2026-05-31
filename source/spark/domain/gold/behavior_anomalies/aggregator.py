from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from domain.gold.common import score_anomaly


def build_behavior_anomalies(
    video_df: DataFrame,
    pdf_df: DataFrame,
    performance_df: DataFrame,
    learning_df: DataFrame,
) -> DataFrame:
    video = score_anomaly(
        video_df.withColumn("action_type", F.lower(F.col("event_type"))),
        anomaly_domain="video",
        entity_type="video_action",
        entity_id_expr=F.concat_ws("|", F.col("video_id"), F.col("action_type")),
        partition_cols=["course_id", "video_id", "action_type"],
        extra_group_cols=["event_date", "course_id", "video_id", "action_type"],
    )

    pdf = score_anomaly(
        pdf_df.withColumn(
            "content_id",
            F.coalesce(F.col("pdf_name"), F.col("doc_url"), F.col("chapter")),
        ),
        anomaly_domain="pdf",
        entity_type="pdf_content",
        entity_id_expr=F.col("content_id"),
        partition_cols=["course_id", "content_id"],
        extra_group_cols=["event_date", "course_id", "content_id"],
    )

    performance = score_anomaly(
        performance_df,
        anomaly_domain="performance",
        entity_type="problem",
        entity_id_expr=F.col("problem_id"),
        partition_cols=["course_id", "problem_id"],
        extra_group_cols=["event_date", "course_id", "problem_id"],
    )

    learning = score_anomaly(
        learning_df.withColumn(
            "journey_entity_id",
            F.concat_ws("|", F.col("course_id"), F.col("user_id").cast("string")),
        ),
        anomaly_domain="journey",
        entity_type="user_course",
        entity_id_expr=F.col("journey_entity_id"),
        partition_cols=["course_id", "user_id"],
        extra_group_cols=["event_date", "course_id", "user_id", "journey_entity_id"],
    )

    return (
        video.unionByName(pdf, allowMissingColumns=True)
        .unionByName(performance, allowMissingColumns=True)
        .unionByName(learning, allowMissingColumns=True)
    )
