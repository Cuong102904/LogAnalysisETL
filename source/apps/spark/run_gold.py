from __future__ import annotations

import argparse

from apps.spark.common import load_profile, load_source_metric, read_records, write_records
from learnlake.metrics import build_course_activity_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LearnLake Gold metric builder.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--metric", default="gold_course_activity_summary")
    parser.add_argument("--input")
    parser.add_argument("--output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profile = load_profile(args.source)
    metric = load_source_metric(args.source, args.metric)
    input_path = args.input or profile.silver.path
    output_path = args.output or f"/tmp/learnlake/{metric.output}"
    silver_records = read_records(input_path)
    if metric.metric_id != "gold_course_activity_summary":
        raise ValueError(f"Unsupported metric for vertical slice: {metric.metric_id}")
    gold_records = build_course_activity_summary(silver_records)
    write_records(output_path, gold_records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
