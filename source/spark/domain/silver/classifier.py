from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_classification(df: DataFrame) -> DataFrame:
    event_source = F.lower(F.get_json_object(F.col("value_raw"), "$.event_source"))
    event_type = F.lower(F.get_json_object(F.col("value_raw"), "$.event_type"))
    name = F.lower(F.get_json_object(F.col("value_raw"), "$.name"))
    path = F.lower(F.get_json_object(F.col("value_raw"), "$.context.path"))

    browser = event_source == "browser"

    browser_learning_core = (
        event_type.contains("video")
        | event_type.contains("seq")
        | event_type.contains("textbook.pdf")
        | event_type.contains("textbook.book")
        | event_type.contains("pdfbook")
        | event_type.contains("book_reader")
        | event_type.contains("link_clicked")
        | event_type.contains("page_close")
        | event_type.contains("jump_to")
        | event_type.contains("courseware")
    )

    instructional_cross_source = (
        event_type.contains("goto_position")
        | event_type.contains("get_completion")
        | event_type.contains("transcript")
        | event_type.contains("translation")
        | event_type.contains("publish_completion")
        | event_type.contains("mark_completed")
        | event_type.contains("edx.video.completed")
        | (event_type.contains("save_user_state") & event_type.contains("+type@video+block@"))
        | event_type.contains("googlecomponent")
        | event_type.contains("google-document")
        | event_type.contains("resume_course.clicked")
        | event_type.contains("librarycontentblock")
        | event_type.contains("/course/")
    )

    learning = (
        browser & (browser_learning_core | (event_type == "book"))
    ) | instructional_cross_source

    performance = (
        (event_type == "edx.grades.problem.submitted")
        | (event_type == "problem_check")
        | (event_type == "problem_graded")
        | event_type.contains("problem_check")
        | event_type.contains("input_ajax")
        | event_type.contains("problem_show")
        | event_type.contains("essay_question")
        | event_type.contains("create_submission")
        | event_type.contains("student_submit")
        | event_type.contains("/handler/student_submit")
        | event_type.contains("problem_save")
        | event_type.contains("showanswer")
        | event_type.contains("quiz")
        | event_type.contains("grade")
        | event_type.contains("progress")
        | event_type.contains("score")
    )

    system = (
        event_type.contains("proctoring")
        | event_type.contains("proctored_exam")
        | event_type.contains("special_exam")
        | event_type.contains("timed.attempt")
        | event_type.contains("heartbeat")
        | event_type.contains("auth")
        | event_type.contains("/auth/")
        | event_type.contains("login")
        | event_type.contains("logout")
        | event_type.contains("/login")
        | event_type.contains("/logout")
        | event_type.contains("/signup")
        | event_type.contains("/signin")
        | event_type.contains("/dashboard")
        | path.contains("/auth/")
        | path.contains("/login")
        | path.contains("/logout")
        | path.contains("/dashboard")
        | name.contains("auth")
        | name.contains("login")
        | path.contains("/api/")
        | event_type.startswith("/api/")
    )

    product = (
        event_type.startswith("edx.bi.")
        | event_type.startswith("edx.segment.")
        | event_type.startswith("edx.dashboard.")
        | event_type.startswith("edx.catalog.")
        | event_type.startswith("edx.partner.")
        | event_type.startswith("edx.student.")
        | event_type.startswith("edx.program.")
        | event_type.startswith("edx.instructor.")
        | (
            event_type.startswith("edx.user.")
            & (
                ~event_type.contains("/login")
                & ~event_type.contains("/logout")
                & ~event_type.contains("auth")
                & ~name.contains("auth")
                & ~path.contains("/auth/")
                & ~path.contains("/login")
                & ~path.contains("/logout")
            )
        )
    )

    navigation_server_paths = event_type.startswith("/courses/") | (event_type == "/")

    edx_product_residual = event_type.startswith("edx.")

    return df.withColumn(
        "silver_class",
        # Performance before learning so /courses handler URLs matching problem_* grade first.
        F.when(performance, F.lit("performance"))
        .when(system, F.lit("system"))
        .when(learning, F.lit("learning"))
        .when(product, F.lit("product"))
        .when(edx_product_residual, F.lit("product"))
        .when(navigation_server_paths, F.lit("navigation"))
        .otherwise(F.lit("unknown")),
    )
