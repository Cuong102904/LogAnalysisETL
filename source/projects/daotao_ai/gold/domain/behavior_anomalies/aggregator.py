from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects.daotao_ai.gold.domain.common import ensure_event_date, score_anomaly
from projects.daotao_ai.gold.schemas.behavior_anomalies import (
    BEHAVIOR_ANOMALY_SIGNALS_SCHEMA,
)


def _with_common_event_columns(df: DataFrame) -> DataFrame:
    return ensure_event_date(
        df.withColumn("user_id", F.col("actor_id").cast("long")).withColumn(
            "time", F.col("event_time")
        )
    )


def build_behavior_anomalies(
    video_df: DataFrame,
    pdf_df: DataFrame,
    performance_df: DataFrame,
    learning_df: DataFrame,
) -> DataFrame:
    video = score_anomaly(
        _with_common_event_columns(video_df).withColumn(
            "action_type", F.lower(F.col("video_action"))
        ),
        anomaly_domain="video",
        entity_type="video_action",
        entity_id_expr=F.concat_ws("|", F.col("video_id"), F.col("action_type")),
        partition_cols=["course_id", "video_id", "action_type"],
        extra_group_cols=["event_date", "course_id", "video_id", "action_type"],
    )

    pdf = score_anomaly(
        _with_common_event_columns(pdf_df).withColumn(
            "content_id",
            F.coalesce(
                F.col("file_name"),
                F.col("asset_url"),
                F.col("document_id"),
                F.col("chapter"),
                F.col("chapter_title"),
            ),
        ),
        anomaly_domain="pdf",
        entity_type="pdf_content",
        entity_id_expr=F.col("content_id"),
        partition_cols=["course_id", "content_id"],
        extra_group_cols=["event_date", "course_id", "content_id"],
    )

    performance = score_anomaly(
        _with_common_event_columns(performance_df).withColumn(
            "problem_type", F.coalesce(F.col("response_type"), F.col("input_type"))
        ),
        anomaly_domain="performance",
        entity_type="problem",
        entity_id_expr=F.col("problem_id"),
        partition_cols=["course_id", "problem_id"],
        extra_group_cols=["event_date", "course_id", "problem_id"],
    )

    learning = score_anomaly(
        _with_common_event_columns(learning_df).filter(
            (F.col("learning_relevance") == F.lit("learning"))
            & (F.coalesce(F.col("is_noise"), F.lit(False)) == F.lit(False))
        ),
        anomaly_domain="journey",
        entity_type="course",
        entity_id_expr=F.col("course_id"),
        partition_cols=["course_id"],
        extra_group_cols=["event_date", "course_id"],
    )

    return (
        video.unionByName(pdf, allowMissingColumns=True)
        .unionByName(performance, allowMissingColumns=True)
        .unionByName(learning, allowMissingColumns=True)
        .select(*[field.name for field in BEHAVIOR_ANOMALY_SIGNALS_SCHEMA])
    )
