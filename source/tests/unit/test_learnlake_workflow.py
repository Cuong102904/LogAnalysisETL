from __future__ import annotations

import pytest

from learnlake.orchestration import WorkflowPlanner
from learnlake.runtime import load_workflow_definition
from learnlake.runtime.config import resolve_path


def test_workflow_definition_plans_parallelizable_stages() -> None:
    workflow = load_workflow_definition(resolve_path("catalog/workflows/daotao_ai_pipeline.yaml"))
    plan = WorkflowPlanner(workflow).plan()

    assert plan.ordered_task_ids[0] == "bronze_ingest"
    assert plan.ordered_task_ids[1] == "silver_normalize"
    assert any(stage.task_ids == ["bronze_ingest"] for stage in plan.stages)
    assert any(stage.task_ids == ["silver_normalize"] for stage in plan.stages)


def test_workflow_definition_rejects_cycles() -> None:
    workflow = load_workflow_definition(resolve_path("catalog/workflows/daotao_ai_pipeline.yaml"))
    cycled = workflow.model_copy(
        update={
            "tasks": [
                task.model_copy(update={"after": ["delta_maintenance"]})
                if task.task_id == "bronze_ingest"
                else task
                for task in workflow.tasks
            ]
        }
    )

    with pytest.raises(ValueError, match="cycle"):
        WorkflowPlanner(cycled).validate()
