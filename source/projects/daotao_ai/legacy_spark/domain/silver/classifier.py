from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_classification(df: DataFrame) -> DataFrame:
    """
    Classify each raw event into a silver_class routing label.

    Priority order (first match wins):
      1. performance  — problem/quiz/grading events
      2. exam         — timed/proctored exam attempt lifecycle
      3. video        — video player interactions
      4. pdf          — textbook PDF and Google Doc interactions
      5. navigation   — sequence navigation + server-side page views
      6. system       — auth, bot/scan, API infra (no real username activity)
      7. learning     — remaining authenticated user events (catch-all)
      8. unknown      — anything unclassified
    """
    event_source = F.lower(F.get_json_object(F.col("value_raw"), "$.event_source"))
    event_type = F.lower(F.get_json_object(F.col("value_raw"), "$.event_type"))
    path = F.lower(F.get_json_object(F.col("value_raw"), "$.context.path"))
    username = F.get_json_object(F.col("value_raw"), "$.username")

    is_authenticated = username.isNotNull() & (username != "")

    # ── performance ──────────────────────────────────────────────────────────
    performance = (
        (event_type == "edx.grades.problem.submitted")
        | (event_type == "problem_check")
        | (event_type == "problem_graded")
        | (event_type == "problem_save")
        | (event_type == "problem_show")
        | (event_type == "showanswer")
        | event_type.contains("edx.courseware.index.report")
        | event_type.contains("input_ajax")
        | event_type.contains("create_submission")
        | event_type.contains("student_submit")
        | event_type.contains("/handler/student_submit")
        | event_type.contains("/handler/get_test_case")  # coderunner
        | event_type.contains("/handler/get_id")  # coderunner
    )

    # ── exam ─────────────────────────────────────────────────────────────────
    exam = event_type.startswith("edx.special_exam.timed.attempt.")

    # ── video ─────────────────────────────────────────────────────────────────
    video = event_type.isin(
        "play_video",
        "pause_video",
        "seek_video",
        "stop_video",
        "speed_change_video",
        "load_video",
    ) | (event_type.contains("save_user_state") & event_type.contains("+type@video+block@"))

    # ── pdf ───────────────────────────────────────────────────────────────────
    pdf = (
        event_type.startswith("textbook.pdf.")
        | (event_type == "book")
        | (event_type == "edx.googlecomponent.document.displayed")
    )

    # ── navigation ────────────────────────────────────────────────────────────
    navigation = (
        event_type.isin("seq_goto", "seq_next", "seq_prev", "page_close")
        | event_type.isin(
            "edx.ui.lms.sequence.next_selected",
            "edx.ui.lms.sequence.previous_selected",
        )
        | event_type.contains("edx.courseware.index.access")
        # server-side page view URLs (authenticated users navigating the LMS)
        | (
            is_authenticated
            & (event_source == "server")
            & (
                event_type.startswith("/courses/")
                | event_type.startswith("/course/")
                | (event_type == "/")
                | event_type.contains("jump_to")
            )
        )
    )

    # ── system ────────────────────────────────────────────────────────────────
    # Unauthenticated requests OR infrastructure/auth paths
    system = (
        ~is_authenticated
        | event_type.contains("proctoring")
        | event_type.contains("proctored_exam")
        | event_type.startswith("/api/edx_proctoring/")
        | event_type.contains("heartbeat")
        | event_type.contains("/auth/")
        | event_type.contains("/login")
        | event_type.contains("/logout")
        | event_type.contains("/signup")
        | event_type.contains("/signin")
        | path.contains("/auth/")
        | path.contains("/login")
        | path.contains("/logout")
        | path.startswith("/api/")
        | event_type.startswith("/api/")
    )

    # ── learning (catch-all for authenticated users) ───────────────────────
    learning = is_authenticated

    return df.withColumn(
        "silver_class",
        F.when(performance, F.lit("performance"))
        .when(exam, F.lit("exam"))
        .when(video, F.lit("video"))
        .when(pdf, F.lit("pdf"))
        .when(navigation, F.lit("navigation"))
        .when(system, F.lit("system"))
        .when(learning, F.lit("learning"))
        .otherwise(F.lit("unknown")),
    )
