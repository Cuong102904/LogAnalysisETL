from __future__ import annotations

import argparse

from projects.daotao_ai.gold.assessment_config import AssessmentBatchConfig
from projects.daotao_ai.gold.pipelines.assessment_batch_pipeline import run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LearnLake assessment gold batch.")
    parser.add_argument("--snapshot-date")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        AssessmentBatchConfig.from_env(),
        snapshot_date=args.snapshot_date,
        start_date=args.start_date,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()
