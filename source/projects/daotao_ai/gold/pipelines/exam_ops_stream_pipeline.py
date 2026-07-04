from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from learnlake.runtime import build_spark
from projects.daotao_ai.gold.domain.exam_ops import (
    FLOW_GROUP_KEYS,
    LOAD_GROUP_KEYS,
    QUESTION_METRIC_MERGE_KEYS,
    TIMELINE_MERGE_KEYS,
    build_exam_ops_base,
    build_exam_windows,
    build_gold_exam_attempt_timeline,
    build_gold_exam_attempt_flow_10s,
    build_gold_exam_load_10s,
    build_gold_exam_question_metrics,
)
from projects.daotao_ai.gold.exam_ops_config import GoldExamOpsConfig


def _merge_condition(keys: list[str]) -> str:
    return " AND ".join(f"t.{key} <=> s.{key}" for key in keys)


def _upsert_delta(
    df: DataFrame,
    path: str,
    merge_keys: list[str],
    *,
    partition_by: str = "event_date",
) -> None:
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, path):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(partition_by)
            .save(path)
        )
        return

    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(df.alias("s"), _merge_condition(merge_keys))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _affected_attempt_ids(batch_df: DataFrame) -> list[str]:
    rows = (
        build_exam_ops_base(batch_df)
        .select(F.col("attempt_id").alias("attempt_id"))
        .dropna()
        .dropDuplicates(["attempt_id"])
        .collect()
    )
    return [row.attempt_id for row in rows]


def _read_delta(spark, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def _attempt_id_expr(df: DataFrame):
    if "attempt_id" in df.columns and "exam_attempt_id" in df.columns:
        return F.coalesce(F.col("attempt_id"), F.col("exam_attempt_id")).cast("string")
    if "attempt_id" in df.columns:
        return F.col("attempt_id").cast("string")
    if "exam_attempt_id" in df.columns:
        return F.col("exam_attempt_id").cast("string")
    raise ValueError("Expected one of attempt_id or exam_attempt_id in exam attempts dataframe")


def _initialize_gold_tables(spark, config: GoldExamOpsConfig) -> None:
    empty_exam_attempts_df = _read_delta(spark, config.input_exam_attempts_path).limit(0)
    empty_events_canonical_df = _read_delta(spark, config.input_events_canonical_path).limit(0)
    empty_problem_submissions_df = _read_delta(spark, config.input_problem_submissions_path).limit(0)
    empty_problem_grades_df = _read_delta(spark, config.input_problem_grades_path).limit(0)

    empty_exam_windows_df = build_exam_windows(empty_exam_attempts_df)
    empty_timeline_df = build_gold_exam_attempt_timeline(
        empty_exam_windows_df,
        empty_events_canonical_df,
        empty_problem_submissions_df.filter(F.col("submission_source") == F.lit("browser")),
    )
    empty_question_metrics_df = build_gold_exam_question_metrics(
        empty_exam_windows_df,
        empty_problem_submissions_df.filter(F.col("submission_source") == F.lit("server")),
        empty_problem_grades_df,
    )

    _upsert_delta(
        empty_timeline_df,
        config.output_exam_attempt_timeline_path,
        TIMELINE_MERGE_KEYS,
    )
    _upsert_delta(
        empty_question_metrics_df,
        config.output_exam_question_metrics_path,
        QUESTION_METRIC_MERGE_KEYS,
    )


def _process_batch(batch_df: DataFrame, batch_id: int, config: GoldExamOpsConfig) -> None:
    if batch_df.isEmpty():
        print(f"{config.query_name} batch_id={batch_id} empty", flush=True)
        return

    base_df = build_exam_ops_base(batch_df)
    if base_df.isEmpty():
        print(f"{config.query_name} batch_id={batch_id} no matching exam attempt events", flush=True)
        return

    load_df = build_gold_exam_load_10s(base_df)
    flow_df = build_gold_exam_attempt_flow_10s(base_df)

    _upsert_delta(load_df, config.output_exam_load_10s_path, LOAD_GROUP_KEYS)
    _upsert_delta(flow_df, config.output_exam_attempt_flow_10s_path, FLOW_GROUP_KEYS)

    attempt_ids = _affected_attempt_ids(batch_df)
    if attempt_ids:
        spark = batch_df.sparkSession
        full_exam_attempts_df = _read_delta(spark, config.input_exam_attempts_path)
        affected_exam_attempts_df = (
            full_exam_attempts_df.withColumn(
                "attempt_id",
                _attempt_id_expr(full_exam_attempts_df),
            )
            .filter(F.col("attempt_id").isin(attempt_ids))
            .drop("attempt_id")
        )
        exam_windows_df = build_exam_windows(affected_exam_attempts_df)

        if not exam_windows_df.isEmpty():
            window_bounds = exam_windows_df.agg(
                F.min("window_start_utc").alias("min_window_start_utc"),
                F.max("window_end_utc").alias("max_window_end_utc"),
            ).collect()[0]
            course_date_keys_df = exam_windows_df.select("event_date", "course_id").dropDuplicates()
            course_user_keys_df = exam_windows_df.select("course_id", "user_id").dropDuplicates()

            min_window_start_utc = window_bounds.min_window_start_utc
            max_window_end_utc = window_bounds.max_window_end_utc

            canonical_df = _read_delta(spark, config.input_events_canonical_path)
            submissions_df = _read_delta(spark, config.input_problem_submissions_path).filter(
                (F.col("event_time_utc") >= F.lit(min_window_start_utc))
                & (F.col("event_time_utc") <= F.lit(max_window_end_utc))
            )
            grades_df = _read_delta(spark, config.input_problem_grades_path).filter(
                (F.col("event_time_utc") >= F.lit(min_window_start_utc))
                & (F.col("event_time_utc") <= F.lit(max_window_end_utc))
            )

            browser_events_df = (
                canonical_df
                .filter((F.col("event_time_utc") >= F.lit(min_window_start_utc)) & (F.col("event_time_utc") <= F.lit(max_window_end_utc)))
                .withColumn("event_date", F.to_date("event_time_utc"))
                .join(F.broadcast(course_date_keys_df), on=["event_date", "course_id"], how="inner")
                .drop("event_date")
            )
            problem_submissions_df = (
                submissions_df
                .join(
                    F.broadcast(course_user_keys_df),
                    on=[
                        submissions_df.course_id == course_user_keys_df.course_id,
                        submissions_df.user_id.cast("string") == course_user_keys_df.user_id,
                    ],
                    how="inner",
                )
                .drop(course_user_keys_df.course_id)
                .drop(course_user_keys_df.user_id)
            )
            problem_grades_df = (
                grades_df
                .join(
                    F.broadcast(course_user_keys_df),
                    on=[
                        grades_df.course_id == course_user_keys_df.course_id,
                        grades_df.user_id.cast("string") == course_user_keys_df.user_id,
                    ],
                    how="inner",
                )
                .drop(course_user_keys_df.course_id)
                .drop(course_user_keys_df.user_id)
            )

            attempt_timeline_df = build_gold_exam_attempt_timeline(
                exam_windows_df,
                browser_events_df,
                problem_submissions_df.filter(F.col("submission_source") == F.lit("browser")),
            )
            if not attempt_timeline_df.isEmpty():
                _upsert_delta(
                    attempt_timeline_df,
                    config.output_exam_attempt_timeline_path,
                    TIMELINE_MERGE_KEYS,
                )

            question_metrics_df = build_gold_exam_question_metrics(
                exam_windows_df,
                problem_submissions_df.filter(F.col("submission_source") == F.lit("server")),
                problem_grades_df,
            )
            if not question_metrics_df.isEmpty():
                _upsert_delta(
                    question_metrics_df,
                    config.output_exam_question_metrics_path,
                    QUESTION_METRIC_MERGE_KEYS,
                )

    print(
        f"{config.query_name} batch_id={batch_id} wrote "
        f"load={config.output_exam_load_10s_path} "
        f"flow={config.output_exam_attempt_flow_10s_path} "
        f"timeline={config.output_exam_attempt_timeline_path} "
        f"question_metrics={config.output_exam_question_metrics_path}",
        flush=True,
    )


def run(config: GoldExamOpsConfig) -> None:
    spark = build_spark(config.app_name)
    _initialize_gold_tables(spark, config)
    exam_attempts_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", config.max_files_per_trigger)
        .load(config.input_exam_attempts_path)
    )

    (
        exam_attempts_stream.writeStream.option("checkpointLocation", config.checkpoint_path)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .foreachBatch(lambda batch_df, batch_id: _process_batch(batch_df, batch_id, config))
        .queryName(config.query_name)
        .start()
        .awaitTermination()
    )
