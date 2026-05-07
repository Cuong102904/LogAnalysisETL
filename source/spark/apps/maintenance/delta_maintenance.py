from __future__ import annotations

import argparse
import json
import os
from typing import Any

from delta.tables import DeltaTable
from infrastructure.spark.session import build_spark

MIN_SAFE_VACUUM_RETENTION_HOURS = 168


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Delta table maintenance on Spark.")
    parser.add_argument("--table-path", required=True)
    parser.add_argument("--optimize", action="store_true")
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument(
        "--vacuum-retention-hours",
        type=int,
        default=MIN_SAFE_VACUUM_RETENTION_HOURS,
    )
    parser.add_argument("--app-name", default="delta_maintenance")
    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> None:
    if not args.optimize and not args.vacuum:
        raise ValueError("At least one maintenance flag is required: --optimize or --vacuum")
    allow_unsafe = os.getenv("ALLOW_UNSAFE_VACUUM_RETENTION", "false").lower() == "true"
    if args.vacuum and args.vacuum_retention_hours < MIN_SAFE_VACUUM_RETENTION_HOURS:
        if not allow_unsafe:
            raise ValueError(
                "VACUUM retention below 168 hours is blocked. "
                "Set ALLOW_UNSAFE_VACUUM_RETENTION=true only for non-production tests."
            )


def _delta_path_sql(table_path: str) -> str:
    escaped_path = table_path.replace("`", "``")
    return f"delta.`{escaped_path}`"


def run(args: argparse.Namespace) -> dict[str, Any]:
    _validate_args(args)
    spark = build_spark(args.app_name)
    result: dict[str, Any] = {
        "table_path": args.table_path,
        "optimize": args.optimize,
        "vacuum": args.vacuum,
        "vacuum_retention_hours": args.vacuum_retention_hours,
    }
    try:
        delta_table = DeltaTable.forPath(spark, args.table_path)
        if args.optimize:
            delta_table.optimize().executeCompaction()
            result["optimize_status"] = "completed"
        if args.vacuum:
            spark.sql(
                f"VACUUM {_delta_path_sql(args.table_path)} "
                f"RETAIN {args.vacuum_retention_hours} HOURS"
            )
            result["vacuum_status"] = "completed"
        return result
    finally:
        spark.stop()


def main() -> None:
    result = run(_parse_args())
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
