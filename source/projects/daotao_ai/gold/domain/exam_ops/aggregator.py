from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.column import Column

from projects.daotao_ai.gold.domain.assessment import ATTEMPT_EVENT_TYPES, build_exam_windows

LOAD_GROUP_KEYS = ["event_date", "bucket_10s", "course_id", "exam_id", "exam_name"]
FLOW_GROUP_KEYS = [*LOAD_GROUP_KEYS, "flow_stage"]
TIMELINE_MERGE_KEYS = ["exam_attempt_id", "canonical_event_id"]
QUESTION_METRIC_MERGE_KEYS = ["exam_attempt_id", "problem_id"]


def _first_available(df: DataFrame, *candidates: str) -> Column:
    available = [F.col(name) for name in candidates if name in df.columns]
    if not available:
        missing = ", ".join(candidates)
        raise ValueError(f"None of the expected columns are present: {missing}")
    if len(available) == 1:
        return available[0]
    return F.coalesce(*available)


def build_exam_ops_base(df: DataFrame) -> DataFrame:
    normalized = (
        df.select(
            F.col("event_time_utc").alias("event_time_utc"),
            F.col("course_id").alias("course_id"),
            F.col("exam_id").cast("string").alias("exam_id"),
            F.col("exam_name").alias("exam_name"),
            _first_available(df, "attempt_id", "exam_attempt_id").cast("string").alias("attempt_id"),
            F.col("user_id").cast("string").alias("user_id"),
            F.col("attempt_event_type").alias("attempt_event_type"),
            F.col("attempt_status").alias("attempt_status"),
        )
        .filter(F.col("event_time_utc").isNotNull())
        .filter(F.col("attempt_id").isNotNull())
        .filter(F.col("attempt_event_type").isin(*ATTEMPT_EVENT_TYPES.values()))
        .withColumn("event_date", F.to_date("event_time_utc"))
        .withColumn(
            "bucket_10s",
            F.to_timestamp(
                F.from_unixtime(F.floor(F.unix_timestamp("event_time_utc") / F.lit(10)) * F.lit(10))
            ),
        )
        .withColumn(
            "is_created",
            F.when(F.col("attempt_event_type") == F.lit(ATTEMPT_EVENT_TYPES["created"]), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_started",
            F.when(F.col("attempt_event_type") == F.lit(ATTEMPT_EVENT_TYPES["started"]), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_ready_to_submit",
            F.when(F.col("attempt_event_type") == F.lit(ATTEMPT_EVENT_TYPES["ready_to_submit"]), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_submitted",
            F.when(F.col("attempt_event_type") == F.lit(ATTEMPT_EVENT_TYPES["submitted"]), F.lit(1)).otherwise(F.lit(0)),
        )
    )
    return normalized


def build_gold_exam_load_10s(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(*LOAD_GROUP_KEYS)
        .agg(
            F.sum("is_created").cast("long").alias("created_count"),
            F.sum("is_started").cast("long").alias("started_count"),
            F.sum("is_ready_to_submit").cast("long").alias("ready_to_submit_count"),
            F.sum("is_submitted").cast("long").alias("submitted_count"),
            F.countDistinct("user_id").cast("long").alias("distinct_users"),
            F.countDistinct("attempt_id").cast("long").alias("distinct_attempts"),
        )
        .withColumn(
            "total_events",
            F.col("created_count")
            + F.col("started_count")
            + F.col("ready_to_submit_count")
            + F.col("submitted_count"),
        )
    )


def build_gold_exam_attempt_flow_10s(df: DataFrame) -> DataFrame:
    stage_frames: list[DataFrame] = []
    for flow_stage, event_type in ATTEMPT_EVENT_TYPES.items():
        stage_frames.append(
            df.filter(F.col("attempt_event_type") == F.lit(event_type))
            .groupBy(*LOAD_GROUP_KEYS)
            .agg(
                F.count(F.lit(1)).cast("long").alias("event_count"),
                F.countDistinct("user_id").cast("long").alias("distinct_users"),
                F.countDistinct("attempt_id").cast("long").alias("distinct_attempts"),
            )
            .withColumn("flow_stage", F.lit(flow_stage))
            .select(*FLOW_GROUP_KEYS, "event_count", "distinct_users", "distinct_attempts")
        )
    flow_df = stage_frames[0]
    for stage_df in stage_frames[1:]:
        flow_df = flow_df.unionByName(stage_df)
    return flow_df


def build_gold_exam_attempt_timeline(
    exam_windows_df: DataFrame,
    browser_events_df: DataFrame,
    browser_submissions_df: DataFrame,
) -> DataFrame:
    window_projection = exam_windows_df.select(
        "event_date",
        "course_id",
        "exam_id",
        "exam_name",
        "exam_attempt_id",
        "user_id",
        "session_id",
        "window_start_utc",
        "window_end_utc",
        "is_submitted",
    )

    timeline_df = (
        browser_events_df.alias("events")
        .filter(F.col("events.event_source") == F.lit("browser"))
        .join(
            F.broadcast(window_projection).alias("windows"),
            on=(
                (F.col("events.course_id") == F.col("windows.course_id"))
                & (F.col("events.user_id").cast("string") == F.col("windows.user_id"))
                & (F.col("events.event_time_utc") >= F.col("windows.window_start_utc"))
                & (F.col("events.event_time_utc") <= F.col("windows.window_end_utc"))
            ),
            how="inner",
        )
        .join(
            browser_submissions_df.alias("submissions"),
            on=F.col("events.event_id") == F.col("submissions.submission_event_id"),
            how="left",
        )
        .select(
            F.to_date(F.col("events.event_time_utc")).alias("event_date"),
            F.col("windows.course_id").alias("course_id"),
            F.col("windows.exam_id").alias("exam_id"),
            F.col("windows.exam_name").alias("exam_name"),
            F.col("windows.exam_attempt_id").alias("exam_attempt_id"),
            F.col("windows.user_id").alias("user_id"),
            F.col("windows.session_id").alias("exam_session_id"),
            F.col("windows.window_start_utc").alias("window_start_utc"),
            F.col("windows.window_end_utc").alias("window_end_utc"),
            F.col("windows.is_submitted").alias("is_submitted"),
            F.col("events.event_id").alias("canonical_event_id"),
            F.col("events.event_time_utc").alias("event_time_utc"),
            F.col("events.event_source").alias("event_source"),
            F.col("events.event_group").alias("event_group"),
            F.col("events.event_subgroup").alias("event_subgroup"),
            F.col("events.event_type").alias("event_type"),
            F.col("events.event_name").alias("event_name"),
            F.col("events.module_usage_key").alias("module_usage_key"),
            F.coalesce(
                F.col("submissions.module_display_name"),
                F.col("events.module_display_name"),
            ).alias("module_display_name"),
            F.col("submissions.answer_payload").alias("answer_payload"),
            F.col("submissions.success").alias("submission_success"),
            F.col("submissions.grade_raw").alias("submission_grade_raw"),
            F.col("submissions.max_grade_raw").alias("submission_max_grade_raw"),
        )
    )

    sequence_window = Window.partitionBy("exam_attempt_id").orderBy("event_time_utc", "canonical_event_id")
    return timeline_df.withColumn("event_sequence_no", F.row_number().over(sequence_window))


def build_gold_exam_question_metrics(
    exam_windows_df: DataFrame,
    server_submissions_df: DataFrame,
    grades_df: DataFrame,
) -> DataFrame:
    window_projection = exam_windows_df.select(
        "event_date",
        "course_id",
        "exam_id",
        "exam_name",
        "exam_attempt_id",
        "user_id",
        "window_start_utc",
        "window_end_utc",
        "is_submitted",
    )

    submissions_in_window = (
        server_submissions_df.alias("submissions")
        .filter(F.col("submissions.submission_source") == F.lit("server"))
        .join(
            F.broadcast(window_projection).alias("windows"),
            on=(
                (F.col("submissions.course_id") == F.col("windows.course_id"))
                & (F.col("submissions.user_id").cast("string") == F.col("windows.user_id"))
                & (F.col("submissions.event_time_utc") >= F.col("windows.window_start_utc"))
                & (F.col("submissions.event_time_utc") <= F.col("windows.window_end_utc"))
            ),
            how="inner",
        )
        .filter(F.col("submissions.problem_id").isNotNull())
        .select(
            F.col("windows.event_date").alias("event_date"),
            F.col("windows.course_id").alias("course_id"),
            F.col("windows.exam_id").alias("exam_id"),
            F.col("windows.exam_name").alias("exam_name"),
            F.col("windows.exam_attempt_id").alias("exam_attempt_id"),
            F.col("windows.user_id").alias("user_id"),
            F.col("windows.window_start_utc").alias("window_start_utc"),
            F.col("windows.window_end_utc").alias("window_end_utc"),
            F.col("windows.is_submitted").alias("is_submitted"),
            F.col("submissions.problem_id").alias("problem_id"),
            F.col("submissions.module_usage_key").alias("module_usage_key"),
            F.col("submissions.module_display_name").alias("module_display_name"),
            F.col("submissions.submission_event_id").alias("submission_event_id"),
            F.col("submissions.event_time_utc").alias("submission_time_utc"),
            F.col("submissions.attempt_no").alias("attempt_no"),
        )
    )

    grades_in_window = (
        grades_df.alias("grades")
        .join(
            F.broadcast(window_projection).alias("windows"),
            on=(
                (F.col("grades.course_id") == F.col("windows.course_id"))
                & (F.col("grades.user_id").cast("string") == F.col("windows.user_id"))
                & (F.col("grades.event_time_utc") >= F.col("windows.window_start_utc"))
                & (F.col("grades.event_time_utc") <= F.col("windows.window_end_utc"))
            ),
            how="inner",
        )
        .filter(F.col("grades.problem_id").isNotNull())
        .select(
            F.col("windows.event_date").alias("event_date"),
            F.col("windows.course_id").alias("course_id"),
            F.col("windows.exam_id").alias("exam_id"),
            F.col("windows.exam_name").alias("exam_name"),
            F.col("windows.exam_attempt_id").alias("exam_attempt_id"),
            F.col("windows.user_id").alias("user_id"),
            F.col("windows.window_start_utc").alias("window_start_utc"),
            F.col("windows.window_end_utc").alias("window_end_utc"),
            F.col("windows.is_submitted").alias("is_submitted"),
            F.col("grades.problem_id").alias("problem_id"),
            F.col("grades.module_usage_key").alias("module_usage_key"),
            F.col("grades.module_display_name").alias("module_display_name"),
            F.col("grades.grade_event_id").alias("grade_event_id"),
            F.col("grades.event_time_utc").alias("grade_time_utc"),
            F.col("grades.is_correct").alias("is_correct"),
            F.col("grades.grade_ratio").alias("grade_ratio"),
        )
    )

    submission_agg = (
        submissions_in_window.groupBy(
            "event_date",
            "course_id",
            "exam_id",
            "exam_name",
            "exam_attempt_id",
            "user_id",
            "window_start_utc",
            "window_end_utc",
            "is_submitted",
            "problem_id",
        )
        .agg(
            F.max("module_usage_key").alias("submission_module_usage_key"),
            F.max("module_display_name").alias("submission_module_display_name"),
            F.count("submission_event_id").cast("long").alias("submission_event_count"),
            F.max("attempt_no").cast("int").alias("max_attempt_no"),
            F.min("submission_time_utc").alias("first_submission_time_utc"),
            F.max("submission_time_utc").alias("last_submission_time_utc"),
        )
    )

    latest_grade_window = Window.partitionBy("exam_attempt_id", "problem_id").orderBy(
        F.col("grade_time_utc").desc(),
        F.col("grade_event_id").desc(),
    )
    latest_grade = (
        grades_in_window.withColumn("grade_rank_desc", F.row_number().over(latest_grade_window))
        .filter(F.col("grade_rank_desc") == 1)
        .select(
            "exam_attempt_id",
            "problem_id",
            F.col("is_correct").alias("final_is_correct"),
            F.col("grade_ratio").alias("final_grade_ratio"),
            F.col("grade_time_utc").alias("final_grade_time_utc"),
        )
    )

    grade_agg = (
        grades_in_window.groupBy(
            "event_date",
            "course_id",
            "exam_id",
            "exam_name",
            "exam_attempt_id",
            "user_id",
            "window_start_utc",
            "window_end_utc",
            "is_submitted",
            "problem_id",
        )
        .agg(
            F.max("module_usage_key").alias("grade_module_usage_key"),
            F.max("module_display_name").alias("grade_module_display_name"),
            F.count("grade_event_id").cast("long").alias("grade_event_count"),
            F.sum(F.when(F.col("is_correct") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).cast("long").alias(
                "correct_grade_event_count"
            ),
            F.sum(F.when(F.col("is_correct") == F.lit(False), F.lit(1)).otherwise(F.lit(0))).cast("long").alias(
                "wrong_grade_event_count"
            ),
            F.avg("grade_ratio").alias("avg_grade_ratio"),
            F.min("grade_ratio").alias("min_grade_ratio"),
            F.max("grade_ratio").alias("max_grade_ratio"),
            F.min("grade_time_utc").alias("first_grade_time_utc"),
            F.max("grade_time_utc").alias("last_grade_time_utc"),
        )
        .join(latest_grade, on=["exam_attempt_id", "problem_id"], how="left")
    )

    return (
        submission_agg.alias("submissions")
        .join(
            grade_agg.alias("grades"),
            on=[
                "event_date",
                "course_id",
                "exam_id",
                "exam_name",
                "exam_attempt_id",
                "user_id",
                "window_start_utc",
                "window_end_utc",
                "is_submitted",
                "problem_id",
            ],
            how="full_outer",
        )
        .select(
            "event_date",
            "course_id",
            "exam_id",
            "exam_name",
            "exam_attempt_id",
            "user_id",
            "window_start_utc",
            "window_end_utc",
            "is_submitted",
            "problem_id",
            F.coalesce(
                F.col("submissions.submission_module_usage_key"),
                F.col("grades.grade_module_usage_key"),
            ).alias("module_usage_key"),
            F.coalesce(
                F.col("submissions.submission_module_display_name"),
                F.col("grades.grade_module_display_name"),
            ).alias("module_display_name"),
            F.coalesce(F.col("submission_event_count"), F.lit(0)).cast("long").alias("submission_event_count"),
            F.col("max_attempt_no").cast("int").alias("max_attempt_no"),
            "first_submission_time_utc",
            "last_submission_time_utc",
            F.coalesce(F.col("grade_event_count"), F.lit(0)).cast("long").alias("grade_event_count"),
            F.coalesce(F.col("correct_grade_event_count"), F.lit(0)).cast("long").alias("correct_grade_event_count"),
            F.coalesce(F.col("wrong_grade_event_count"), F.lit(0)).cast("long").alias("wrong_grade_event_count"),
            "avg_grade_ratio",
            "min_grade_ratio",
            "max_grade_ratio",
            "final_is_correct",
            "final_grade_ratio",
            "final_grade_time_utc",
            "first_grade_time_utc",
            "last_grade_time_utc",
        )
    )
