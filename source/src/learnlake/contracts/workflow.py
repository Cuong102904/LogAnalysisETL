from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class WorkflowTaskSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str
    runner: Literal["spark", "python", "airflow"]
    entrypoint: str
    after: list[str] = Field(default_factory=list)
    stage: str | None = None
    priority: int = 100
    parallelizable: bool = True
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)


class WorkflowDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str
    source_id: str
    tasks: list[WorkflowTaskSpec]

    @model_validator(mode="after")
    def validate_unique_tasks(self) -> "WorkflowDefinition":
        task_ids = [task.task_id for task in self.tasks]
        duplicates = {task_id for task_id in task_ids if task_ids.count(task_id) > 1}
        if duplicates:
            duplicate_list = ", ".join(sorted(duplicates))
            raise ValueError(f"workflow tasks must be unique, duplicates: {duplicate_list}")
        return self
