from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from learnlake.runtime import build_spark
from projects.daotao_ai.gold.assessment_config import AssessmentStreamConfig
from projects.daotao_ai.gold.domain.assessment import (
    build_exam_session_events,
    build_exam_session_snapshot,
    build_exam_windows,
)


SNAPSHOT_MERGE_KEYS = ["course_id", "user_id", "exam_attempt_id"]
EVENT_MERGE_KEYS = ["event_date", "course_id", "user_id", "exam_attempt_id", "event_time_utc", "event_type", "problem_id"]


def _merge_condition(keys: list[str]) -> str:
    return " AND ".join(f"t.{key} <=> s.{key}" for key in keys)


def _upsert_delta(df: DataFrame, path: str, merge_keys: list[str], *, partition_by: str = "event_date") -> None:
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


def _read_delta(spark, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def _affected_attempt_ids(batch_df: DataFrame) -> list[str]:
    rows = (
        batch_df.select(
            F.coalesce(F.col("attempt_id"), F.col("exam_attempt_id")).cast("string").alias("attempt_id")
        )
        .dropna()
        .dropDuplicates(["attempt_id"])
        .collect()
    )
    return [row.attempt_id for row in rows]


def _initialize_outputs(spark, config: AssessmentStreamConfig) -> None:
    empty_exam_attempts_df = _read_delta(spark, config.input_exam_attempts_path).limit(0)
    empty_problem_submissions_df = _read_delta(spark, config.input_problem_submissions_path).limit(0)
    empty_exam_windows_df = build_exam_windows(empty_exam_attempts_df)
    empty_snapshot_df = build_exam_session_snapshot(empty_exam_attempts_df, empty_problem_submissions_df)
    empty_events_df = build_exam_session_events(empty_exam_attempts_df, empty_problem_submissions_df)
    _upsert_delta(empty_snapshot_df, config.output_exam_session_snapshot_path, SNAPSHOT_MERGE_KEYS)
    _upsert_delta(empty_events_df, config.output_exam_session_events_path, EVENT_MERGE_KEYS)


def _process_batch(batch_df: DataFrame, batch_id: int, config: AssessmentStreamConfig) -> None:
    if batch_df.isEmpty():
        print(f"{config.query_name} batch_id={batch_id} empty", flush=True)
        return

    attempt_ids = _affected_attempt_ids(batch_df)
    if not attempt_ids:
        print(f"{config.query_name} batch_id={batch_id} no attempt ids", flush=True)
        return

    spark = batch_df.sparkSession
    full_exam_attempts_df = _read_delta(spark, config.input_exam_attempts_path)
    affected_exam_attempts_df = (
        full_exam_attempts_df.withColumn(
            "attempt_id",
            F.coalesce(F.col("attempt_id"), F.col("exam_attempt_id")).cast("string"),
        )
        .filter(F.col("attempt_id").isin(attempt_ids))
        .drop("attempt_id")
    )
    exam_windows_df = build_exam_windows(affected_exam_attempts_df)
    if exam_windows_df.isEmpty():
        print(f"{config.query_name} batch_id={batch_id} no exam windows", flush=True)
        return

    window_bounds = exam_windows_df.agg(
        F.min("window_start_utc").alias("min_window_start_utc"),
        F.max("window_end_utc").alias("max_window_end_utc"),
    ).collect()[0]
    min_window_start_utc = window_bounds.min_window_start_utc
    max_window_end_utc = window_bounds.max_window_end_utc

    full_problem_submissions_df = _read_delta(spark, config.input_problem_submissions_path)
    course_user_keys_df = exam_windows_df.select("course_id", "user_id").dropDuplicates()
    problem_submissions_df = (
        full_problem_submissions_df.filter(
            (F.col("event_time_utc") >= F.lit(min_window_start_utc))
            & (F.col("event_time_utc") <= F.lit(max_window_end_utc))
        )
        .join(
            F.broadcast(course_user_keys_df),
            on=[
                full_problem_submissions_df.course_id == course_user_keys_df.course_id,
                full_problem_submissions_df.user_id.cast("string") == course_user_keys_df.user_id,
            ],
            how="inner",
        )
        .drop(course_user_keys_df.course_id)
        .drop(course_user_keys_df.user_id)
    )

    snapshot_df = build_exam_session_snapshot(affected_exam_attempts_df, problem_submissions_df)
    events_df = build_exam_session_events(affected_exam_attempts_df, problem_submissions_df)
    if not snapshot_df.isEmpty():
        _upsert_delta(snapshot_df, config.output_exam_session_snapshot_path, SNAPSHOT_MERGE_KEYS)
    if not events_df.isEmpty():
        _upsert_delta(events_df, config.output_exam_session_events_path, EVENT_MERGE_KEYS)

    print(
        f"{config.query_name} batch_id={batch_id} wrote "
        f"snapshot={config.output_exam_session_snapshot_path} "
        f"events={config.output_exam_session_events_path}",
        flush=True,
    )


def run(config: AssessmentStreamConfig) -> None:
    spark = build_spark(config.app_name)
    _initialize_outputs(spark, config)
    exam_attempts_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", config.max_files_per_trigger)
        .load(config.input_exam_attempts_path)
    )
    (
        exam_attempts_stream.writeStream.option("checkpointLocation", config.checkpoint_path)
        .trigger(availableNow=True)
        .foreachBatch(lambda batch_df, batch_id: _process_batch(batch_df, batch_id, config))
        .queryName(config.query_name)
        .start()
        .awaitTermination()
    )
