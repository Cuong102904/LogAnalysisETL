from __future__ import annotations

import argparse
import runpy
import sys
from pathlib import Path

from apps.spark.common import load_profile
from learnlake.orchestration import WorkflowPlanner
from learnlake.runtime import load_workflow_definition
from learnlake.runtime.config import resolve_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a configured workflow task.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--workflow")
    parser.add_argument("--task")
    parser.add_argument("--plan", action="store_true")
    return parser.parse_args()


def _workflow_path(source_id: str, workflow_id: str | None) -> Path:
    profile = load_profile(source_id)
    if not profile.workflows:
        raise ValueError(f"Source {source_id} has no declared workflows")
    if workflow_id is None:
        if len(profile.workflows) != 1:
            raise ValueError(
                f"Source {source_id} declares multiple workflows; pass --workflow explicitly"
            )
        return resolve_path(profile.workflows[0])

    for workflow_path in profile.workflows:
        resolved = resolve_path(workflow_path)
        definition = load_workflow_definition(resolved)
        if definition.workflow_id == workflow_id:
            return resolved
    raise ValueError(f"Workflow {workflow_id} is not declared by source {source_id}")


def main() -> int:
    args = parse_args()
    workflow = load_workflow_definition(_workflow_path(args.source, args.workflow))
    planner = WorkflowPlanner(workflow)
    planner.validate()
    plan = planner.plan()
    if args.plan:
        for stage in plan.stages:
            print(f"stage {stage.stage_index}: {', '.join(stage.task_ids)}")
        return 0
    if not args.task:
        raise ValueError("--task is required unless --plan is used")

    task = planner.task(args.task)
    sys.argv = [task.entrypoint, *task.args]
    runpy.run_path(str(resolve_path(task.entrypoint)), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
