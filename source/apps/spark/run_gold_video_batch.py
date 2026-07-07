from __future__ import annotations

import argparse

from projects.daotao_ai.gold.pipelines.video_batch_pipeline import run
from projects.daotao_ai.gold.video_config import VideoGoldConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LearnLake video gold batch.")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(VideoGoldConfig.from_env(), start_date=args.start_date, end_date=args.end_date)


if __name__ == "__main__":
    main()
