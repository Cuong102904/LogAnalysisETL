from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType

from learnlake.contracts import SourceProfile
from learnlake.runtime import (
    load_quality_rules,
    load_silver_parser_config,
    load_silver_routing_config,
    load_source_profile,
)
from learnlake.runtime.config import resolve_path
from learnlake.silver.compiler import compile_quality_rules, compile_routes
from learnlake.silver.schemas import SilverParserConfig, SilverRoutingConfig
from learnlake.silver_domain.transforms import (
    PARSER_REGISTRY,
    attach_event_identity,
    attach_route_fields,
    build_events_canonical,
    build_invalid_events,
    build_unknown_events,
    parse_authentication_flags,
    parse_base_fields,
    parse_context_fields,
)


@dataclass(frozen=True)
class SilverPlan:
    profile: SourceProfile
    routing: SilverRoutingConfig
    parsers: SilverParserConfig
    compiled_routes: list
    compiled_quality_rules: list


def load_silver_plan(source_id: str) -> SilverPlan:
    profile = load_source_profile(source_id)
    routing = load_silver_routing_config(resolve_path(profile.silver.routing))
    parsers = load_silver_parser_config(resolve_path(profile.silver.parsers))
    rules = load_quality_rules([resolve_path(path) for path in profile.silver.quality_rules]).rules
    parser_names = {parser.name for parser in parsers.parsers}
    target_tables = {profile.silver.canonical.table, profile.silver.unknown.table, profile.silver.invalid.table}
    target_tables.update(target.table for target in profile.silver.targets.values())
    for route in routing.routes:
        if route.parser not in parser_names:
            raise ValueError(f"Route {route.id} references undefined parser {route.parser}")
        if route.parser not in PARSER_REGISTRY:
            raise ValueError(f"Route {route.id} references unimplemented parser {route.parser}")
        for target in route.targets.domain:
            if target not in target_tables:
                raise ValueError(f"Route {route.id} references undeclared target table {target}")
    return SilverPlan(
        profile=profile,
        routing=routing,
        parsers=parsers,
        compiled_routes=compile_routes(routing),
        compiled_quality_rules=compile_quality_rules(rules),
    )


def _empty_with_schema(spark: SparkSession, schema: StructType) -> DataFrame:
    return spark.createDataFrame([], schema)


def _apply_quality_rules(df: DataFrame, compiled_rules: list) -> DataFrame:
    if not compiled_rules:
        return (
            df.withColumn("quality_rank", F.lit(0))
            .withColumn("quality_status", F.lit("valid"))
            .withColumn("validation_errors", F.array().cast(ArrayType(StringType())))
        )

    score_map = {"warning": 1, "ignored": 2, "invalid": 3}
    score_columns = [F.when(rule.condition, F.lit(score_map[rule.rule.status])).otherwise(F.lit(0)) for rule in compiled_rules]
    error_columns = [
        F.when(rule.condition, F.lit(rule.rule.message or rule.rule.id)).otherwise(F.lit(None).cast("string"))
        for rule in compiled_rules
    ]
    quality_rank = F.greatest(*score_columns) if len(score_columns) > 1 else score_columns[0]
    return (
        df.withColumn("quality_rank", quality_rank)
        .withColumn(
            "quality_status",
            F.when(F.col("quality_rank") == 3, F.lit("invalid"))
            .when(F.col("quality_rank") == 2, F.lit("ignored"))
            .when(F.col("quality_rank") == 1, F.lit("warning"))
            .otherwise(F.lit("valid")),
        )
        .withColumn(
            "validation_errors",
            F.filter(F.array(*error_columns), lambda value: value.isNotNull()),
        )
    )


def _domain_invalid_split(df: DataFrame, required_columns: list[str], parser_family: str, reason: str) -> tuple[DataFrame, DataFrame]:
    missing_conditions = [F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "") for column in required_columns]
    condition = missing_conditions[0]
    for extra in missing_conditions[1:]:
        condition = condition | extra
    invalid = (
        df.filter(condition)
        .withColumn("parser_family", F.lit(parser_family))
        .withColumn("invalid_reason", F.lit(reason))
        .withColumn("validation_errors", F.array(F.lit(reason)))
        .withColumn("raw_json", F.lit(None).cast("string"))
    )
    valid = df.filter(~condition)
    return valid, invalid


def _table_paths(profile: SourceProfile) -> dict[str, str]:
    paths = {
        profile.silver.canonical.table: profile.silver.canonical.path,
        profile.silver.unknown.table: profile.silver.unknown.path,
        profile.silver.invalid.table: profile.silver.invalid.path,
    }
    paths.update({target.table: target.path for target in profile.silver.targets.values()})
    return paths


def silver_output_paths(profile: SourceProfile) -> dict[str, str]:
    return _table_paths(profile)

def transform_bronze_batch(
    batch_df: DataFrame,
    plan: SilverPlan,
) -> dict[str, DataFrame]:
    prepared = (
        parse_base_fields(batch_df)
        .transform(parse_context_fields)
        .transform(parse_authentication_flags)
        .transform(attach_event_identity)
        .transform(lambda df: attach_route_fields(df, plan.compiled_routes, plan.routing.version))
    )

    unknown_df = build_unknown_events(prepared.filter(F.col("route_id").isNull()))
    matched_raw = prepared.filter(F.col("route_id").isNotNull())
    canonical_candidate = build_events_canonical(matched_raw)
    canonical_scored = _apply_quality_rules(canonical_candidate, plan.compiled_quality_rules)
    canonical_invalid = canonical_scored.filter(F.col("quality_status") == "invalid").withColumn(
        "invalid_reason", F.lit("quality_validation_failed")
    )
    invalid_frames = [build_invalid_events(canonical_invalid)]
    canonical_valid = (
        canonical_scored.filter(F.col("quality_status") != "invalid")
        .drop("quality_rank", "quality_status", "validation_errors", "raw_json")
    )
    matched_valid = matched_raw.join(canonical_valid.select("event_id"), on="event_id", how="inner")

    browser_submissions_raw = matched_valid.filter(F.col("parser_name") == "parse_problem_check_browser")
    server_submissions_raw = matched_valid.filter(F.col("parser_name") == "parse_problem_check_server")
    grades_raw = matched_valid.filter(F.col("parser_name") == "parse_problem_grade")
    special_exam_raw = matched_valid.filter(F.col("parser_name") == "parse_special_exam_attempt")

    browser_submissions, invalid_browser = _domain_invalid_split(
        PARSER_REGISTRY["parse_problem_check_browser"](browser_submissions_raw),
        ["submission_event_id"],
        "problem_browser",
        "missing_problem_submission_fields",
    )
    invalid_frames.append(build_invalid_events(invalid_browser))

    server_submissions, invalid_server = _domain_invalid_split(
        PARSER_REGISTRY["parse_problem_check_server"](server_submissions_raw),
        ["submission_event_id"],
        "problem_server",
        "missing_problem_submission_fields",
    )
    invalid_frames.append(build_invalid_events(invalid_server))

    problem_grades, invalid_grades = _domain_invalid_split(
        PARSER_REGISTRY["parse_problem_grade"](grades_raw),
        ["grade_event_id"],
        "grade_server",
        "missing_problem_grade_fields",
    )
    invalid_frames.append(build_invalid_events(invalid_grades))

    exam_attempts, invalid_exam_attempts = _domain_invalid_split(
        PARSER_REGISTRY["parse_special_exam_attempt"](special_exam_raw),
        ["exam_attempt_event_id", "exam_attempt_id"],
        "special_exam",
        "missing_exam_attempt_fields",
    )
    invalid_frames.append(build_invalid_events(invalid_exam_attempts))

    video_raw = matched_valid.filter(F.col("parser_name") == "parse_video_interaction")
    video_interactions, invalid_video = _domain_invalid_split(
        PARSER_REGISTRY["parse_video_interaction"](video_raw),
        ["video_event_id", "video_id"],
        "video_browser",
        "missing_video_fields",
    )
    invalid_frames.append(build_invalid_events(invalid_video))

    navigation_raw = matched_valid.filter(F.col("parser_name") == "parse_navigation_event")
    navigation_events, invalid_navigation = _domain_invalid_split(
        PARSER_REGISTRY["parse_navigation_event"](navigation_raw),
        ["event_time_utc", "nav_type"],
        "navigation",
        "missing_navigation_fields",
    )
    invalid_frames.append(build_invalid_events(invalid_navigation))

    content_raw = matched_valid.filter(F.col("parser_name") == "parse_content_access_event")
    content_access_events, invalid_content = _domain_invalid_split(
        PARSER_REGISTRY["parse_content_access_event"](content_raw),
        ["event_time_utc", "content_type"],
        "pdf_book",
        "missing_content_access_fields",
    )
    invalid_frames.append(build_invalid_events(invalid_content))

    auth_noise_raw = matched_valid.filter(F.col("parser_name") == "parse_auth_noise_event")
    auth_noise_events = PARSER_REGISTRY["parse_auth_noise_event"](auth_noise_raw)
    path_noise_raw = matched_valid.filter(F.col("parser_name") == "parse_system_noise_event")
    system_noise_events = auth_noise_events.unionByName(
        PARSER_REGISTRY["parse_system_noise_event"](path_noise_raw),
        allowMissingColumns=True,
    )

    invalid_df = None
    for frame in invalid_frames:
        invalid_df = frame if invalid_df is None else invalid_df.unionByName(frame, allowMissingColumns=True)
    if invalid_df is None:
        invalid_df = _empty_with_schema(batch_df.sparkSession, StructType([]))

    outputs = {
        plan.profile.silver.canonical.table: canonical_valid,
        plan.profile.silver.unknown.table: unknown_df,
        plan.profile.silver.invalid.table: invalid_df,
    }
    if browser_submissions.schema:
        problem_submissions = browser_submissions.unionByName(server_submissions, allowMissingColumns=True)
        outputs["problem_submissions"] = problem_submissions
    if problem_grades.schema:
        outputs["problem_grades"] = problem_grades
    if exam_attempts.schema:
        outputs["exam_attempts"] = exam_attempts
    if video_interactions.schema:
        outputs["video_interactions"] = video_interactions
    if navigation_events.schema:
        outputs["navigation_events"] = navigation_events
    if content_access_events.schema:
        outputs["content_access_events"] = content_access_events
    if system_noise_events.schema:
        outputs["system_noise_events"] = system_noise_events
    return outputs


def write_output_tables(outputs: dict[str, DataFrame], paths_by_table: dict[str, str]) -> None:
    for table_name, df in outputs.items():
        path = paths_by_table.get(table_name)
        if path is None:
            continue
        if not df.columns:
            continue
        df.write.format("delta").mode("append").save(path)
