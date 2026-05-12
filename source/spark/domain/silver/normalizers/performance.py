from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_performance(df: DataFrame) -> DataFrame:
    """
    Normalize problem/quiz/grading events.

    Primary source for scores: edx.grades.problem.submitted (JSON payload).
    Primary source for exam summary: edx.courseware.index.report (JSON payload).
    problem_check: only used to count attempts — payload is URL-encoded, not parsed.
    problem_graded: payload is HTML — not parsed, not stored here.

    problem_type is extracted from block_id URL pattern (type@coderunner vs type@problem).
    """
    base = df.filter(F.col("silver_class") == "performance")
    ev = F.from_json(F.get_json_object("value_raw", "$.event"), "map<string,string>")

    # Extract problem_type from URL path: type@coderunner vs type@problem
    path_col = F.get_json_object("value_raw", "$.context.path")
    problem_type = (
        F.when(path_col.contains("+type@coderunner+"), F.lit("coderunner"))
        .when(path_col.contains("+type@problem+"), F.lit("mcq"))
        .otherwise(F.lit(None))
    )

    return base.select(
        F.col("dedup_key").alias("event_id"),
        F.to_timestamp(F.get_json_object("value_raw", "$.time")).alias("ts"),
        F.to_date(F.to_timestamp(F.get_json_object("value_raw", "$.time"))).alias(
            "event_date"
        ),
        F.get_json_object("value_raw", "$.event_type").alias("event_type"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        F.get_json_object("value_raw", "$.context.user_id")
        .cast("long")
        .alias("user_id"),
        F.get_json_object("value_raw", "$.session").alias("session_id"),
        F.get_json_object("value_raw", "$.context.course_id").alias("course_id"),
        F.get_json_object("value_raw", "$.context.org_id").alias("org_id"),
        # problem identity
        ev.getItem("problem_id").alias("problem_id"),
        problem_type.alias("problem_type"),
        # score fields — from edx.grades.problem.submitted only, null otherwise
        ev.getItem("weighted_earned").cast("double").alias("weighted_earned"),
        ev.getItem("weighted_possible").cast("double").alias("weighted_possible"),
        # exam summary fields — from edx.courseware.index.report only, null otherwise
        ev.getItem("total_questions").cast("int").alias("total_questions"),
        ev.getItem("submitted_questions").cast("int").alias("submitted_questions"),
        ev.getItem("correct_questions").cast("int").alias("correct_questions"),
    )
