from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column

ATTEMPT_EVENT_TYPES = {
    "created": "edx.special_exam.timed.attempt.created",
    "started": "edx.special_exam.timed.attempt.started",
    "ready_to_submit": "edx.special_exam.timed.attempt.ready_to_submit",
    "submitted": "edx.special_exam.timed.attempt.submitted",
}

ATTEMPT_EVENT_LABELS = {value: key for key, value in ATTEMPT_EVENT_TYPES.items()}


def _first_available(df: DataFrame, *candidates: str) -> Column:
    available = [F.col(name) for name in candidates if name in df.columns]
    if not available:
        missing = ", ".join(candidates)
        raise ValueError(f"None of the expected columns are present: {missing}")
    if len(available) == 1:
        return available[0]
    return F.coalesce(*available)


def _maybe_column(df: DataFrame, name: str, dtype: str = "string") -> Column:
    if name in df.columns:
        return F.col(name)
    return F.lit(None).cast(dtype)


def build_exam_windows(df: DataFrame) -> DataFrame:
    attempt_id = _first_available(df, "attempt_id", "exam_attempt_id").cast("string")
    user_id = _first_available(df, "attempt_user_id", "user_id").cast("string")
    base = (
        df.select(
            F.col("event_time_utc"),
            F.col("course_id").alias("course_id"),
            F.col("exam_id").cast("string").alias("exam_id"),
            F.col("exam_name").alias("exam_name"),
            attempt_id.alias("exam_attempt_id"),
            user_id.alias("user_id"),
            _maybe_column(df, "session_id").alias("session_id"),
            F.col("attempt_event_type").alias("attempt_event_type"),
            _maybe_column(df, "started_time_utc", "timestamp").alias("started_time_utc"),
            _maybe_column(df, "submitted_time_utc", "timestamp").alias("submitted_time_utc"),
        )
        .filter(F.col("exam_attempt_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
    )
    group_keys = ["course_id", "exam_id", "exam_name", "exam_attempt_id", "user_id"]
    session_summary = base.groupBy(*group_keys).agg(
        F.max(F.struct(F.col("event_time_utc"), F.col("session_id"))).getField("session_id").alias("session_id"),
        F.min("event_time_utc").alias("first_attempt_event_time_utc"),
        F.max("event_time_utc").alias("last_attempt_event_time_utc"),
    )
    started_summary = (
        base.filter(F.col("attempt_event_type") == F.lit(ATTEMPT_EVENT_TYPES["started"]))
        .groupBy(*group_keys)
        .agg(
            F.min(
                F.when(
                    F.col("started_time_utc").isNotNull()
                    | (F.col("attempt_event_type") == F.lit(ATTEMPT_EVENT_TYPES["started"])),
                    F.coalesce(F.col("started_time_utc"), F.col("event_time_utc")),
                )
            ).alias("window_start_utc")
        )
    )
    submitted_summary = (
        base.filter(
            (F.col("attempt_event_type") == F.lit(ATTEMPT_EVENT_TYPES["submitted"]))
            | F.col("submitted_time_utc").isNotNull()
        )
        .groupBy(*group_keys)
        .agg(
            F.max(
                F.when(
                    F.col("submitted_time_utc").isNotNull()
                    | (F.col("attempt_event_type") == F.lit(ATTEMPT_EVENT_TYPES["submitted"])),
                    F.coalesce(F.col("submitted_time_utc"), F.col("event_time_utc")),
                )
            ).alias("window_end_utc"),
            F.max(
                F.when(
                    F.col("submitted_time_utc").isNotNull()
                    | (F.col("attempt_event_type") == F.lit(ATTEMPT_EVENT_TYPES["submitted"])),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("is_submitted"),
        )
    )
    return (
        session_summary.join(started_summary, on=group_keys, how="left")
        .join(submitted_summary, on=group_keys, how="left")
        .withColumn(
            "window_start_utc",
            F.coalesce(F.col("window_start_utc"), F.col("first_attempt_event_time_utc")),
        )
        .withColumn(
            "window_end_utc",
            F.coalesce(F.col("window_end_utc"), F.col("last_attempt_event_time_utc")),
        )
        .withColumn("is_submitted", F.col("is_submitted").cast("boolean"))
        .withColumn("is_submitted", F.coalesce(F.col("is_submitted"), F.lit(False)))
        .filter(F.col("window_end_utc") >= F.col("window_start_utc"))
        .withColumn("event_date", F.to_date("window_start_utc"))
    )


def classify_with_exam_windows(
    df: DataFrame,
    exam_windows_df: DataFrame,
    *,
    time_col: str = "event_time_utc",
) -> DataFrame:
    if "course_id" not in df.columns or "user_id" not in df.columns or time_col not in df.columns:
        raise ValueError("Expected course_id, user_id, and event_time_utc columns for context classification")

    window_projection = exam_windows_df.select(
        "course_id",
        "user_id",
        "exam_attempt_id",
        "window_start_utc",
        "window_end_utc",
    )
    join_condition = (
        (F.col("source.course_id") == F.col("windows.course_id"))
        & (F.col("source.user_id").cast("string") == F.col("windows.user_id").cast("string"))
        & (F.col(f"source.{time_col}") >= F.col("windows.window_start_utc"))
        & (F.col(f"source.{time_col}") <= F.col("windows.window_end_utc"))
    )
    source_columns = [F.col(f"source.{name}").alias(name) for name in df.columns]
    classified = (
        df.alias("source")
        .join(F.broadcast(window_projection).alias("windows"), join_condition, how="left")
        .select(
            *source_columns,
            F.col("windows.exam_attempt_id").alias("matched_exam_attempt_id"),
            F.col("windows.window_start_utc").alias("matched_window_start_utc"),
            F.col("windows.window_end_utc").alias("matched_window_end_utc"),
        )
        .withColumn(
            "exam_attempt_id",
            F.coalesce(_maybe_column(df, "exam_attempt_id"), F.col("matched_exam_attempt_id")),
        )
        .withColumn(
            "context",
            F.when(F.col("matched_exam_attempt_id").isNotNull(), F.lit("exam")).otherwise(F.lit("practice")),
        )
        .withColumn("in_exam_window", F.col("matched_exam_attempt_id").isNotNull())
    )
    return classified


def classify_problem_submissions(submissions_df: DataFrame, exam_windows_df: DataFrame) -> DataFrame:
    classified = classify_with_exam_windows(submissions_df, exam_windows_df)
    if "attempt_no" not in classified.columns:
        classified = classified.withColumn("attempt_no", F.lit(None).cast("int"))
    return classified


def classify_problem_grades(grades_df: DataFrame, exam_windows_df: DataFrame) -> DataFrame:
    classified = classify_with_exam_windows(grades_df, exam_windows_df)
    if "is_correct" not in classified.columns:
        classified = classified.withColumn("is_correct", F.lit(None).cast("boolean"))
    if "grade_ratio" not in classified.columns:
        classified = classified.withColumn("grade_ratio", F.lit(None).cast("double"))
    return classified


def resolve_course_mode(exam_windows_df: DataFrame, classified_submissions_df: DataFrame) -> DataFrame:
    exam_courses = exam_windows_df.select("course_id").distinct().withColumn("has_exam", F.lit(1))
    context_flags = (
        classified_submissions_df.select("course_id", "context")
        .dropDuplicates()
        .groupBy("course_id")
        .agg(
            F.max(F.when(F.col("context") == F.lit("exam"), F.lit(1)).otherwise(F.lit(0))).alias("has_exam_context"),
            F.max(F.when(F.col("context") == F.lit("practice"), F.lit(1)).otherwise(F.lit(0))).alias(
                "has_practice_context"
            ),
        )
    )
    return (
        exam_courses.join(context_flags, on="course_id", how="full_outer")
        .withColumn("has_exam", F.coalesce(F.col("has_exam"), F.lit(0)))
        .withColumn("has_exam_context", F.coalesce(F.col("has_exam_context"), F.lit(0)))
        .withColumn("has_practice_context", F.coalesce(F.col("has_practice_context"), F.lit(0)))
        .withColumn(
            "course_mode",
            F.when((F.col("has_exam") == 1) & (F.col("has_practice_context") == 1), F.lit("mixed"))
            .when((F.col("has_exam") == 1) | (F.col("has_exam_context") == 1), F.lit("exam"))
            .otherwise(F.lit("practice")),
        )
        .select("course_id", "course_mode")
    )


def _latest_struct(*cols: str) -> Column:
    return F.max(F.struct(*[F.col(col) for col in cols]))


def build_exam_session_snapshot(exam_attempts_df: DataFrame, problem_submissions_df: DataFrame) -> DataFrame:
    exam_windows_df = build_exam_windows(exam_attempts_df)
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

    attempt_summary = (
        exam_attempts_df.select(
            "event_time_utc",
            "course_id",
            F.coalesce(_maybe_column(exam_attempts_df, "attempt_user_id"), F.col("user_id")).cast("string").alias(
                "user_id"
            ),
            F.coalesce(_maybe_column(exam_attempts_df, "attempt_id"), _maybe_column(exam_attempts_df, "exam_attempt_id")).cast(
                "string"
            ).alias("exam_attempt_id"),
            "attempt_status",
            "attempt_event_type",
        )
        .filter(F.col("exam_attempt_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .groupBy("course_id", "user_id", "exam_attempt_id")
        .agg(
            _latest_struct("event_time_utc", "attempt_status", "attempt_event_type").getField("attempt_status").alias(
                "current_status"
            ),
            _latest_struct("event_time_utc", "attempt_status", "attempt_event_type").getField(
                "attempt_event_type"
            ).alias("current_event_type"),
            F.max("event_time_utc").alias("last_event_time_utc"),
        )
    )

    submission_context = (
        classify_problem_submissions(problem_submissions_df, exam_windows_df)
        .filter(F.col("context") == F.lit("exam"))
        .select(
            "event_time_utc",
            "course_id",
            "user_id",
            "exam_attempt_id",
            "problem_id",
            "submission_event_id",
            "attempt_no",
        )
    )
    submission_summary = (
        submission_context.groupBy("course_id", "user_id", "exam_attempt_id")
        .agg(
            F.count("submission_event_id").cast("long").alias("submission_count"),
            F.sum(F.greatest(F.coalesce(F.col("attempt_no"), F.lit(1)) - F.lit(1), F.lit(0))).cast("long").alias(
                "retry_count"
            ),
            _latest_struct("event_time_utc", "problem_id", "submission_event_id").getField("problem_id").alias(
                "last_problem_id"
            ),
            F.max("event_time_utc").alias("last_problem_time_utc"),
        )
    )

    return (
        window_projection.join(attempt_summary, on=["course_id", "user_id", "exam_attempt_id"], how="left")
        .join(submission_summary, on=["course_id", "user_id", "exam_attempt_id"], how="left")
        .withColumn("exam_started_at", F.col("window_start_utc"))
        .withColumn("exam_submitted_at", F.col("window_end_utc"))
        .withColumn("submission_count", F.coalesce(F.col("submission_count"), F.lit(0)).cast("long"))
        .withColumn("retry_count", F.coalesce(F.col("retry_count"), F.lit(0)).cast("long"))
        .withColumn("last_problem_id", F.col("last_problem_id"))
        .withColumn("last_event_time_utc", F.greatest(F.col("last_event_time_utc"), F.col("last_problem_time_utc")))
        .withColumn(
            "current_status",
            F.coalesce(
                F.col("current_status"),
                F.when(F.col("exam_submitted_at").isNotNull(), F.lit("submitted")).otherwise(F.lit("active")),
            ),
        )
        .withColumn("is_active", F.col("current_status") != F.lit("submitted"))
        .select(
            "event_date",
            "course_id",
            "user_id",
            "exam_attempt_id",
            "exam_id",
            "exam_name",
            "exam_started_at",
            "exam_submitted_at",
            "last_event_time_utc",
            "last_problem_id",
            "submission_count",
            "retry_count",
            "current_status",
            "is_active",
        )
    )


def build_exam_session_events(exam_attempts_df: DataFrame, problem_submissions_df: DataFrame) -> DataFrame:
    exam_windows_df = build_exam_windows(exam_attempts_df)
    attempt_events = (
        exam_attempts_df.select(
            F.to_date("event_time_utc").alias("event_date"),
            "course_id",
            F.coalesce(_maybe_column(exam_attempts_df, "attempt_user_id"), F.col("user_id")).cast("string").alias(
                "user_id"
            ),
            F.coalesce(_maybe_column(exam_attempts_df, "attempt_id"), _maybe_column(exam_attempts_df, "exam_attempt_id")).cast(
                "string"
            ).alias("exam_attempt_id"),
            F.lit(None).cast("string").alias("problem_id"),
            F.col("attempt_event_type").alias("source_event_type"),
            "event_time_utc",
            F.lit("exam").alias("context"),
            F.lit(None).cast("int").alias("attempt_no"),
        )
        .filter(F.col("exam_attempt_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .withColumn("event_type", F.coalesce(F.col("source_event_type"), F.lit("unknown_exam_event")))
        .drop("source_event_type")
    )
    submission_events = (
        classify_problem_submissions(problem_submissions_df, exam_windows_df)
        .filter(F.col("context") == F.lit("exam"))
        .select(
            F.to_date("event_time_utc").alias("event_date"),
            "course_id",
            "user_id",
            "exam_attempt_id",
            "problem_id",
            F.lit("problem_submission").alias("event_type"),
            "event_time_utc",
            "context",
            "attempt_no",
        )
    )
    return attempt_events.unionByName(submission_events, allowMissingColumns=True)


def build_assessment_problem_daily_stats(
    exam_windows_df: DataFrame,
    submissions_df: DataFrame,
    grades_df: DataFrame,
    *,
    course_mode_submissions_df: DataFrame | None = None,
) -> DataFrame:
    classified_submissions = classify_problem_submissions(submissions_df, exam_windows_df)
    classified_grades = classify_problem_grades(grades_df, exam_windows_df)
    course_mode_source = course_mode_submissions_df if course_mode_submissions_df is not None else submissions_df
    course_mode_classified = classify_problem_submissions(course_mode_source, exam_windows_df)

    group_keys = ["event_date", "course_id", "problem_id", "context"]
    submission_stats = (
        classified_submissions.groupBy(*group_keys)
        .agg(
            F.count(F.lit(1)).cast("long").alias("submission_count"),
            F.countDistinct("user_id").cast("long").alias("distinct_students"),
            F.sum(F.greatest(F.coalesce(F.col("attempt_no"), F.lit(1)) - F.lit(1), F.lit(0))).cast("long").alias(
                "retry_count"
            ),
            F.max(F.col("attempt_no").cast("int")).cast("int").alias("max_attempt_no"),
        )
    )
    grade_stats = (
        classified_grades.groupBy(*group_keys)
        .agg(
            F.sum(F.when(F.col("is_correct") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).cast("long").alias(
                "correct_count"
            ),
            F.sum(F.when(F.col("is_correct") == F.lit(False), F.lit(1)).otherwise(F.lit(0))).cast("long").alias(
                "wrong_count"
            ),
        )
    )
    course_mode_df = resolve_course_mode(exam_windows_df, course_mode_classified)

    return (
        submission_stats.join(grade_stats, on=group_keys, how="full_outer")
        .join(course_mode_df, on="course_id", how="left")
        .withColumn("course_mode", F.coalesce(F.col("course_mode"), F.lit("practice")))
        .withColumn("submission_count", F.coalesce(F.col("submission_count"), F.lit(0)).cast("long"))
        .withColumn("distinct_students", F.coalesce(F.col("distinct_students"), F.lit(0)).cast("long"))
        .withColumn("retry_count", F.coalesce(F.col("retry_count"), F.lit(0)).cast("long"))
        .withColumn("max_attempt_no", F.col("max_attempt_no").cast("int"))
        .withColumn("correct_count", F.coalesce(F.col("correct_count"), F.lit(0)).cast("long"))
        .withColumn("wrong_count", F.coalesce(F.col("wrong_count"), F.lit(0)).cast("long"))
        .select(
            "event_date",
            "course_id",
            "problem_id",
            "context",
            "course_mode",
            "submission_count",
            "distinct_students",
            "retry_count",
            "max_attempt_no",
            "correct_count",
            "wrong_count",
        )
    )
