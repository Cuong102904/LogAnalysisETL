from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from learnlake.runtime import build_spark
from projects.daotao_ai.gold.assessment_config import AssessmentBatchConfig
from projects.daotao_ai.gold.domain.assessment import (
    build_assessment_problem_daily_stats,
    build_exam_windows,
)


@dataclass(frozen=True)
class AssessmentBatchRange:
    snapshot_date: date | None = None
    start_date: date | None = None
    end_date: date | None = None

    @property
    def effective_start(self) -> date | None:
        return self.snapshot_date or self.start_date

    @property
    def effective_end(self) -> date | None:
        return self.snapshot_date or self.end_date


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


def _filter_by_range(df: DataFrame, start_date: date | None, end_date: date | None) -> DataFrame:
    filtered = df
    if start_date is not None:
        filtered = filtered.filter(F.col("event_date") >= F.lit(start_date.isoformat()))
    if end_date is not None:
        filtered = filtered.filter(F.col("event_date") <= F.lit(end_date.isoformat()))
    return filtered


def _parse_date(value: str | None) -> date | None:
    if value in (None, ""):
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def run(config: AssessmentBatchConfig, *, snapshot_date: str | None = None, start_date: str | None = None, end_date: str | None = None) -> None:
    spark = build_spark(config.app_name)
    range_spec = AssessmentBatchRange(
        snapshot_date=_parse_date(snapshot_date),
        start_date=_parse_date(start_date),
        end_date=_parse_date(end_date),
    )

    exam_attempts_df = _read_delta(spark, config.input_exam_attempts_path)
    problem_submissions_df = _read_delta(spark, config.input_problem_submissions_path)
    problem_grades_df = _read_delta(spark, config.input_problem_grades_path)

    exam_windows_df = build_exam_windows(exam_attempts_df)
    filtered_submissions_df = _filter_by_range(
        problem_submissions_df,
        range_spec.effective_start,
        range_spec.effective_end,
    )
    filtered_grades_df = _filter_by_range(
        problem_grades_df,
        range_spec.effective_start,
        range_spec.effective_end,
    )
    daily_stats_df = build_assessment_problem_daily_stats(
        exam_windows_df,
        filtered_submissions_df,
        filtered_grades_df,
        course_mode_submissions_df=problem_submissions_df,
    )

    _upsert_delta(daily_stats_df, config.output_problem_daily_stats_path, ["event_date", "course_id", "problem_id", "context", "course_mode"])
    print(
        f"{config.query_name} wrote {config.output_problem_daily_stats_path} "
        f"range={range_spec.effective_start}..{range_spec.effective_end}",
        flush=True,
    )
