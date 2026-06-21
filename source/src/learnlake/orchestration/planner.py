from __future__ import annotations

from collections import defaultdict, deque

from pydantic import BaseModel, ConfigDict

from learnlake.contracts.workflow import WorkflowDefinition, WorkflowTaskSpec


class WorkflowStage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage_index: int
    task_ids: list[str]


class PlannedWorkflow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str
    source_id: str
    ordered_task_ids: list[str]
    stages: list[WorkflowStage]


class WorkflowPlanner:
    def __init__(self, definition: WorkflowDefinition):
        self.definition = definition
        self._task_map = {task.task_id: task for task in definition.tasks}

    def validate(self) -> None:
        missing_dependencies: dict[str, list[str]] = {}
        for task in self.definition.tasks:
            missing = [dependency for dependency in task.after if dependency not in self._task_map]
            if missing:
                missing_dependencies[task.task_id] = missing
        if missing_dependencies:
            formatted = ", ".join(
                f"{task_id} -> {sorted(missing)}"
                for task_id, missing in sorted(missing_dependencies.items())
            )
            raise ValueError(f"workflow contains missing dependencies: {formatted}")

        self._topological_order()

    def plan(self) -> PlannedWorkflow:
        ordered = self._topological_order()
        depth_by_task = self._depths(ordered)
        stage_map: dict[int, list[str]] = defaultdict(list)
        for task_id in ordered:
            stage_map[depth_by_task[task_id]].append(task_id)

        stages = [
            WorkflowStage(stage_index=index, task_ids=sorted(task_ids))
            for index, task_ids in sorted(stage_map.items())
        ]
        return PlannedWorkflow(
            workflow_id=self.definition.workflow_id,
            source_id=self.definition.source_id,
            ordered_task_ids=ordered,
            stages=stages,
        )

    def task(self, task_id: str) -> WorkflowTaskSpec:
        return self._task_map[task_id]

    def _topological_order(self) -> list[str]:
        in_degree: dict[str, int] = {task.task_id: 0 for task in self.definition.tasks}
        graph: dict[str, list[str]] = defaultdict(list)
        for task in self.definition.tasks:
            for dependency in task.after:
                graph[dependency].append(task.task_id)
                in_degree[task.task_id] += 1

        queue = deque(
            sorted(
                (task_id for task_id, degree in in_degree.items() if degree == 0),
                key=lambda task_id: (self._task_map[task_id].priority, task_id),
            )
        )
        ordered: list[str] = []
        while queue:
            task_id = queue.popleft()
            ordered.append(task_id)
            for child in sorted(
                graph[task_id],
                key=lambda child_id: (self._task_map[child_id].priority, child_id),
            ):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(ordered) != len(self.definition.tasks):
            raise ValueError("workflow contains a dependency cycle")
        return ordered

    def _depths(self, ordered: list[str]) -> dict[str, int]:
        depth_by_task: dict[str, int] = {}
        for task_id in ordered:
            task = self._task_map[task_id]
            if not task.after:
                depth_by_task[task_id] = 0
                continue
            depth_by_task[task_id] = max(depth_by_task[dependency] for dependency in task.after) + 1
        return depth_by_task
