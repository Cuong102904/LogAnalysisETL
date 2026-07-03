from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType


def _json(path_column: str, json_path: str):
    return F.get_json_object(F.col(path_column), json_path)


def _json_double(path_column: str, json_path: str):
    return _json(path_column, json_path).cast("double")


def _json_long(path_column: str, json_path: str):
    return _json(path_column, json_path).cast("long")


def _json_bool(path_column: str, json_path: str):
    return _json(path_column, json_path).cast("boolean")


def _first_csv_token(column_name: str):
    return F.trim(F.element_at(F.split(F.col(column_name), ","), 1))


def parse_base_fields(df: DataFrame) -> DataFrame:
    raw_json_col = F.col("raw_payload")
    return (
        df.withColumn("raw_json", raw_json_col)
        .withColumn("event_time_utc", F.coalesce(F.col("event_time"), F.to_timestamp(_json("raw_payload", "$.time"))))
        .withColumn("event_date", F.to_date(F.col("event_time_utc")))
        .withColumn("event_hour_utc", F.hour(F.col("event_time_utc")))
        .withColumn("username", _json("raw_payload", "$.username"))
        .withColumn("session_id", _json("raw_payload", "$.session"))
        .withColumn("ip", _json("raw_payload", "$.ip"))
        .withColumn("agent", _json("raw_payload", "$.agent"))
        .withColumn("host", _json("raw_payload", "$.host"))
        .withColumn("page", _json("raw_payload", "$.page"))
        .withColumn("referer", _json("raw_payload", "$.referer"))
        .withColumn("event_source", _json("raw_payload", "$.event_source"))
        .withColumn("event_type", _json("raw_payload", "$.event_type"))
        .withColumn("event_name", F.coalesce(_json("raw_payload", "$.name"), _json("raw_payload", "$.event_type")))
        .withColumn("context_json", _json("raw_payload", "$.context"))
        .withColumn("event_json", _json("raw_payload", "$.event"))
    )


def parse_context_fields(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("course_id", _json("context_json", "$.course_id"))
        .withColumn("org_id", _json("context_json", "$.org_id"))
        .withColumn("user_id", _json("context_json", "$.user_id").cast("string"))
        .withColumn("context_path", _json("context_json", "$.path"))
        .withColumn("module_usage_key", _json("context_json", "$.module.usage_key"))
        .withColumn("module_display_name", _json("context_json", "$.module.display_name"))
    )


def parse_authentication_flags(df: DataFrame) -> DataFrame:
    username = F.lower(F.trim(F.coalesce(F.col("username"), F.lit(""))))
    return df.withColumn(
        "is_authenticated",
        ((username != "") & (~username.isin("anonymous", "none", "null")))
        | F.col("user_id").isNotNull(),
    )


def attach_event_identity(df: DataFrame) -> DataFrame:
    return df.withColumn("event_id", F.col("event_id"))


def attach_route_fields(df: DataFrame, compiled_routes, routing_version: str) -> DataFrame:
    route_id = None
    event_group = None
    event_subgroup = None
    parser_family = None
    parser_name = None
    is_noise = None

    for compiled in compiled_routes:
        cond = compiled.condition
        route = compiled.route
        route_id = F.when(cond, F.lit(route.id)) if route_id is None else route_id.when(cond, F.lit(route.id))
        event_group = (
            F.when(cond, F.lit(route.classify.event_group))
            if event_group is None
            else event_group.when(cond, F.lit(route.classify.event_group))
        )
        event_subgroup = (
            F.when(cond, F.lit(route.classify.event_subgroup))
            if event_subgroup is None
            else event_subgroup.when(cond, F.lit(route.classify.event_subgroup))
        )
        parser_family = (
            F.when(cond, F.lit(route.classify.parser_family))
            if parser_family is None
            else parser_family.when(cond, F.lit(route.classify.parser_family))
        )
        parser_name = (
            F.when(cond, F.lit(route.parser))
            if parser_name is None
            else parser_name.when(cond, F.lit(route.parser))
        )
        is_noise = (
            F.when(cond, F.lit(route.classify.is_noise))
            if is_noise is None
            else is_noise.when(cond, F.lit(route.classify.is_noise))
        )

    return (
        df.withColumn("route_id", route_id.otherwise(F.lit(None).cast("string")) if route_id is not None else F.lit(None).cast("string"))
        .withColumn("event_group", event_group.otherwise(F.lit(None).cast("string")) if event_group is not None else F.lit(None).cast("string"))
        .withColumn("event_subgroup", event_subgroup.otherwise(F.lit(None).cast("string")) if event_subgroup is not None else F.lit(None).cast("string"))
        .withColumn("parser_family", parser_family.otherwise(F.lit(None).cast("string")) if parser_family is not None else F.lit(None).cast("string"))
        .withColumn("parser_name", parser_name.otherwise(F.lit(None).cast("string")) if parser_name is not None else F.lit(None).cast("string"))
        .withColumn("is_noise", is_noise.otherwise(F.lit(False)) if is_noise is not None else F.lit(False))
        .withColumn("route_version", F.lit(routing_version))
    )


def build_events_canonical(df: DataFrame) -> DataFrame:
    return df.select(
        "event_id",
        "event_time_utc",
        "event_date",
        "event_hour_utc",
        "username",
        "user_id",
        "session_id",
        "ip",
        "agent",
        "host",
        "page",
        "referer",
        "event_source",
        "event_type",
        "event_name",
        "course_id",
        "org_id",
        "context_path",
        "module_usage_key",
        "module_display_name",
        "event_json",
        "event_group",
        "event_subgroup",
        "parser_family",
        "is_noise",
        "is_authenticated",
        "route_id",
        "route_version",
        "raw_json",
    )


def build_unknown_events(df: DataFrame) -> DataFrame:
    return df.select(
        "event_id",
        "event_time_utc",
        "event_date",
        "username",
        "user_id",
        "session_id",
        "course_id",
        "event_source",
        "event_type",
        "event_name",
        "context_path",
        F.lit("no_route_matched").alias("unknown_reason"),
        "route_version",
        F.lit("unresolved").alias("replay_status"),
        F.current_timestamp().alias("first_seen_at"),
        F.lit(None).cast("timestamp").alias("last_replayed_at"),
        F.lit(None).cast("timestamp").alias("resolved_at"),
        F.lit(None).cast("string").alias("resolved_route_id"),
        F.col("raw_json").alias("raw_json"),
    )


def build_invalid_events(df: DataFrame, invalid_reason_col: str = "invalid_reason") -> DataFrame:
    def _maybe_col(name: str, dtype: str = "string"):
        return F.col(name) if name in df.columns else F.lit(None).cast(dtype).alias(name)

    event_id_col = None
    for candidate in ("event_id", "submission_event_id", "grade_event_id", "exam_attempt_event_id", "video_event_id"):
        if candidate in df.columns:
            event_id_col = F.col(candidate)
            break
    if event_id_col is None:
        event_id_col = F.lit(None).cast("string").alias("event_id")

    raw_json_col = _maybe_col("raw_json")
    return df.select(
        event_id_col.alias("event_id"),
        "event_time_utc",
        _maybe_col("event_source"),
        _maybe_col("event_type"),
        _maybe_col("course_id"),
        _maybe_col("session_id"),
        _maybe_col("parser_family"),
        F.col(invalid_reason_col).alias("invalid_reason"),
        F.col("validation_errors").alias("validation_errors"),
        raw_json_col.alias("raw_json"),
        F.current_timestamp().alias("failed_at"),
    )


def parse_problem_check_browser(df: DataFrame) -> DataFrame:
    module_usage_key = _json("context_json", "$.module.usage_key")
    return df.select(
        F.col("event_id").alias("submission_event_id"),
        "event_time_utc",
        "username",
        "user_id",
        "session_id",
        "course_id",
        F.lit(None).cast("string").alias("problem_id"),
        module_usage_key.alias("module_usage_key"),
        "module_display_name",
        F.lit("browser").alias("submission_source"),
        _first_csv_token("event_json").alias("answer_payload"),
        F.lit(None).cast("int").alias("attempt_no"),
        _json("event_json", "$.success").alias("success"),
        _json_double("event_json", "$.grade").alias("grade_raw"),
        _json_double("event_json", "$.max_grade").alias("max_grade_raw"),
        _json("event_json", "$.question_variant").alias("question_variant"),
        F.lit(False).alias("in_exam_window"),
        F.lit(None).cast("string").alias("exam_attempt_id"),
    )


def parse_problem_check_server(df: DataFrame) -> DataFrame:
    answers_json = F.to_json(
        F.from_json(_json("event_json", "$.answers"), MapType(StringType(), StringType()))
    )
    submission_json = _json("event_json", "$.submission")
    return df.select(
        F.col("event_id").alias("submission_event_id"),
        "event_time_utc",
        "username",
        "user_id",
        "session_id",
        "course_id",
        _json("event_json", "$.problem_id").alias("problem_id"),
        F.col("module_usage_key"),
        "module_display_name",
        F.lit("server").alias("submission_source"),
        F.coalesce(answers_json, submission_json, F.col("event_json")).alias("answer_payload"),
        _json_long("event_json", "$.attempts").cast("int").alias("attempt_no"),
        _json("event_json", "$.success").alias("success"),
        _json_double("event_json", "$.grade").alias("grade_raw"),
        _json_double("event_json", "$.max_grade").alias("max_grade_raw"),
        _json("event_json", "$.question_variant").alias("question_variant"),
        F.lit(False).alias("in_exam_window"),
        F.lit(None).cast("string").alias("exam_attempt_id"),
    )


def parse_problem_grade(df: DataFrame) -> DataFrame:
    weighted_earned = _json_double("event_json", "$.weighted_earned")
    weighted_possible = _json_double("event_json", "$.weighted_possible")
    return df.select(
        F.col("event_id").alias("grade_event_id"),
        "event_time_utc",
        "username",
        "user_id",
        "session_id",
        "course_id",
        _json("event_json", "$.problem_id").alias("problem_id"),
        "module_usage_key",
        "module_display_name",
        weighted_earned.alias("weighted_earned"),
        weighted_possible.alias("weighted_possible"),
        F.when(weighted_possible > F.lit(0), weighted_earned >= weighted_possible).otherwise(F.lit(None)).alias("is_correct"),
        F.when(weighted_possible > F.lit(0), weighted_earned / weighted_possible).otherwise(F.lit(None)).alias("grade_ratio"),
        F.lit(None).cast("string").alias("exam_attempt_id"),
        F.lit(False).alias("in_exam_window"),
    )


def parse_special_exam_attempt(df: DataFrame) -> DataFrame:
    started_at = F.to_timestamp(_json("event_json", "$.attempt_started_at"))
    submitted_at = F.to_timestamp(_json("event_json", "$.attempt_completed_at"))
    elapsed_secs = _json_double("event_json", "$.attempt_event_elapsed_time_secs")
    return df.select(
        F.col("event_id").alias("exam_attempt_event_id"),
        "event_time_utc",
        F.col("event_type").alias("attempt_event_type"),
        "username",
        "user_id",
        _json("event_json", "$.attempt_user_id").cast("string").alias("attempt_user_id"),
        "session_id",
        "course_id",
        F.coalesce(_json("event_json", "$.attempt_id"), _json("event_json", "$.exam_attempt_id")).cast("string").alias("exam_attempt_id"),
        _json("event_json", "$.exam_id").cast("string").alias("exam_id"),
        _json("event_json", "$.exam_name").alias("exam_name"),
        _json("event_json", "$.exam_content_id").alias("exam_content_id"),
        _json("event_json", "$.attempt_code").alias("attempt_code"),
        F.col("event_time_utc").alias("created_time_utc"),
        started_at.alias("started_time_utc"),
        submitted_at.alias("submitted_time_utc"),
        elapsed_secs.alias("attempt_event_elapsed_time_secs"),
        _json_long("event_json", "$.attempt_allowed_time_limit_mins").cast("int").alias("allowed_time_limit_mins"),
        _json_long("event_json", "$.exam_default_time_limit_mins").cast("int").alias("exam_default_time_limit_mins"),
        _json("event_json", "$.attempt_status").alias("attempt_status"),
        _json_bool("event_json", "$.exam_is_active").alias("exam_is_active"),
        _json_bool("event_json", "$.exam_is_proctored").alias("is_proctored"),
        _json_bool("event_json", "$.exam_is_practice_exam").alias("is_practice_exam"),
    )


def parse_video_interaction(df: DataFrame) -> DataFrame:
    current_time = F.coalesce(_json_double("event_json", "$.currentTime"), _json_double("event_json", "$.current_time"))
    old_time = _json_double("event_json", "$.old_time")
    new_time = _json_double("event_json", "$.new_time")
    duration = _json_double("event_json", "$.duration")
    old_speed = _json_double("event_json", "$.old_speed")
    new_speed = F.coalesce(_json_double("event_json", "$.new_speed"), _json_double("event_json", "$.speed"))
    speed = new_speed
    position = F.when(F.col("event_type") == "seek_video", new_time).otherwise(current_time)
    return df.select(
        F.col("event_id").alias("video_event_id"),
        "event_time_utc",
        "username",
        "user_id",
        "session_id",
        "course_id",
        _json("event_json", "$.id").alias("video_id"),
        F.coalesce(F.col("module_usage_key"), _json("event_json", "$.code")).alias("video_block_id"),
        _json("event_json", "$.code").alias("video_code"),
        F.col("event_type").alias("action_type"),
        duration.alias("duration_s"),
        current_time.alias("current_time_s"),
        old_time.alias("old_time_s"),
        new_time.alias("new_time_s"),
        old_speed.alias("old_speed"),
        new_speed.alias("new_speed"),
        speed.alias("speed"),
        F.when(old_time.isNotNull() & new_time.isNotNull(), F.lit("seek")).otherwise(F.lit(None)).alias("seek_type"),
        F.when(duration > F.lit(0), position / duration).otherwise(F.lit(None)).alias("watch_ratio"),
        (F.floor(position / F.lit(10)) * F.lit(10)).cast("int").alias("position_bucket_10s"),
        (F.floor(position / F.lit(30)) * F.lit(30)).cast("int").alias("position_bucket_30s"),
    )


def parse_transcript_request(df: DataFrame) -> DataFrame:
    return df.select(
        "event_id",
        "event_time_utc",
        "course_id",
        "user_id",
        "session_id",
        "context_path",
        F.regexp_extract("context_path", r"/handler/transcript/([^/?]+)", 1).alias("language_code"),
    )


def parse_navigation_event(df: DataFrame) -> DataFrame:
    navigation_event_id = F.coalesce(_json("event_json", "$.id"), F.col("event_id"))
    old_tab = _json_long("event_json", "$.old").cast("int")
    new_tab = _json_long("event_json", "$.new").cast("int")
    target_tab = _json_long("event_json", "$.target_tab").cast("int")
    widget_placement = _json("event_json", "$.widget_placement")
    position = _json_long("event_json", "$.POST.position").cast("int")
    usage_key = F.coalesce(
        _json("event_json", "$.POST.usage_key"),
        _json("event_json", "$.POST.usage_key[0]"),
    )
    return df.select(
        navigation_event_id.alias("navigation_event_id"),
        "event_time_utc",
        "username",
        "session_id",
        "course_id",
        F.col("event_type").alias("nav_type"),
        F.col("event_name").alias("nav_name"),
        _json("event_json", "$.from_block").alias("from_block"),
        F.coalesce(_json("event_json", "$.to_block"), _json("event_json", "$.id"), usage_key).alias("to_block"),
        old_tab.alias("old_tab"),
        new_tab.alias("new_tab"),
        target_tab.alias("target_tab"),
        _json_long("event_json", "$.current_tab").cast("int").alias("current_tab"),
        _json_long("event_json", "$.tab_count").cast("int").alias("tab_count"),
        widget_placement.alias("widget_placement"),
        position.alias("position"),
        usage_key.alias("usage_key"),
        "page",
        "referer",
    )


def parse_content_access_event(df: DataFrame) -> DataFrame:
    return df.select(
        "event_time_utc",
        "username",
        "session_id",
        "course_id",
        F.when(F.col("event_type") == "book", F.lit("book")).otherwise(F.lit("pdf")).alias("content_type"),
        F.coalesce(_json("event_json", "$.name"), F.col("event_name")).alias("content_event_name"),
        _json("event_json", "$.chapter").alias("chapter"),
        _json_long("event_json", "$.page").cast("int").alias("page_no"),
        _json("event_json", "$.direction").alias("direction"),
        _json_long("event_json", "$.old").cast("int").alias("old_page"),
        _json_long("event_json", "$.new").cast("int").alias("new_page"),
        _json_double("event_json", "$.amount").alias("zoom_amount"),
    )


def parse_auth_noise_event(df: DataFrame) -> DataFrame:
    return df.select(
        "event_time_utc",
        "host",
        "ip",
        "agent",
        "event_type",
        "context_path",
        F.lit("auth").alias("noise_family"),
        (F.trim(F.coalesce(F.col("username"), F.lit(""))) == "").alias("username_empty_flag"),
    )


def parse_system_noise_event(df: DataFrame) -> DataFrame:
    noise_family = F.when(F.col("context_path").rlike(r"/(robots\.txt|wp-login\.php|xmlrpc\.php|\.git|\.env)"), F.lit("path_scan")).otherwise(F.lit("bot"))
    return df.select(
        "event_time_utc",
        "host",
        "ip",
        "agent",
        "event_type",
        "context_path",
        noise_family.alias("noise_family"),
        (F.trim(F.coalesce(F.col("username"), F.lit(""))) == "").alias("username_empty_flag"),
    )


PARSER_REGISTRY = {
    "parse_problem_check_browser": parse_problem_check_browser,
    "parse_problem_check_server": parse_problem_check_server,
    "parse_problem_grade": parse_problem_grade,
    "parse_special_exam_attempt": parse_special_exam_attempt,
    "parse_video_interaction": parse_video_interaction,
    "parse_transcript_request": parse_transcript_request,
    "parse_navigation_event": parse_navigation_event,
    "parse_content_access_event": parse_content_access_event,
    "parse_auth_noise_event": parse_auth_noise_event,
    "parse_system_noise_event": parse_system_noise_event,
}
