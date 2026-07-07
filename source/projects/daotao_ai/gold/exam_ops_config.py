from __future__ import annotations

from dataclasses import dataclass

from projects.daotao_ai.gold.config_loader import env_int, env_str, load_app_config


@dataclass(frozen=True)
class GoldExamOpsConfig:
    app_name: str
    input_exam_attempts_path: str
    input_events_canonical_path: str
    input_problem_submissions_path: str
    input_problem_grades_path: str
    output_exam_load_10s_path: str
    output_exam_attempt_flow_10s_path: str
    output_exam_attempt_timeline_path: str
    output_exam_question_metrics_path: str
    checkpoint_path: str
    query_name: str
    trigger_interval_seconds: int
    max_files_per_trigger: int

    @classmethod
    def from_env(cls) -> GoldExamOpsConfig:
        config = load_app_config("GOLD_EXAM_OPS_CONFIG_PATH", "gold_exam_ops_stream.yaml")
        return cls(
            app_name=env_str(
                "GOLD_EXAM_OPS_APP_NAME",
                config,
                "app_name",
                default="gold_exam_ops_stream",
            ),
            input_exam_attempts_path=env_str(
                "SILVER_EXAM_ATTEMPTS_PATH",
                config,
                "input",
                "silver_exam_attempts_path",
                default="s3a://lakehouse/learnlake/silver/exam_attempts",
            ),
            input_events_canonical_path=env_str(
                "SILVER_EVENTS_CANONICAL_PATH",
                config,
                "input",
                "silver_events_canonical_path",
                default="s3a://lakehouse/learnlake/silver/events_canonical",
            ),
            input_problem_submissions_path=env_str(
                "SILVER_PROBLEM_SUBMISSIONS_PATH",
                config,
                "input",
                "silver_problem_submissions_path",
                default="s3a://lakehouse/learnlake/silver/problem_submissions",
            ),
            input_problem_grades_path=env_str(
                "SILVER_PROBLEM_GRADES_PATH",
                config,
                "input",
                "silver_problem_grades_path",
                default="s3a://lakehouse/learnlake/silver/problem_grades",
            ),
            output_exam_load_10s_path=env_str(
                "GOLD_EXAM_LOAD_10S_PATH",
                config,
                "storage",
                "gold_exam_load_10s_path",
                default="s3a://lakehouse/learnlake/gold/gold_exam_load_10s",
            ),
            output_exam_attempt_flow_10s_path=env_str(
                "GOLD_EXAM_ATTEMPT_FLOW_10S_PATH",
                config,
                "storage",
                "gold_exam_attempt_flow_10s_path",
                default="s3a://lakehouse/learnlake/gold/gold_exam_attempt_flow_10s",
            ),
            output_exam_attempt_timeline_path=env_str(
                "GOLD_EXAM_ATTEMPT_TIMELINE_PATH",
                config,
                "storage",
                "gold_exam_attempt_timeline_path",
                default="s3a://lakehouse/learnlake/gold/gold_exam_attempt_timeline",
            ),
            output_exam_question_metrics_path=env_str(
                "GOLD_EXAM_QUESTION_METRICS_PATH",
                config,
                "storage",
                "gold_exam_question_metrics_path",
                default="s3a://lakehouse/learnlake/gold/gold_exam_question_metrics",
            ),
            checkpoint_path=env_str(
                "GOLD_EXAM_OPS_CHECKPOINT_PATH",
                config,
                "checkpoint",
                "path",
                default="s3a://platform/learnlake/checkpoints/gold/daotao_ai/exam_ops",
            ),
            query_name=env_str(
                "GOLD_EXAM_OPS_QUERY_NAME",
                config,
                "streaming",
                "query_name",
                default="gold_exam_ops_stream",
            ),
            trigger_interval_seconds=env_int(
                "GOLD_EXAM_OPS_TRIGGER_INTERVAL_SECONDS",
                config,
                "streaming",
                "trigger_interval_seconds",
                default=300,
            ),
            max_files_per_trigger=env_int(
                "GOLD_EXAM_OPS_MAX_FILES_PER_TRIGGER",
                config,
                "options",
                "max_files_per_trigger",
                default=100000,
            ),
        )
