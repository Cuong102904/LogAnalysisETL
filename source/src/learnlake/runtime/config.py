from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from learnlake.contracts import (
    MappingSpec,
    MetricDefinition,
    QualityRuleSet,
    RouteSet,
    SourceProfile,
    WorkflowDefinition,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
CATALOG_ROOT = Path(os.getenv("LEARNLAKE_CATALOG_ROOT", REPO_ROOT / "catalog"))


def resolve_path(path: str | Path, *, base: Path | None = None) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    root = base or REPO_ROOT
    return root / candidate


def load_yaml(path: str | Path) -> dict[str, Any]:
    resolved = resolve_path(path)
    with resolved.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML config at {resolved} must be an object")
    return data


def load_source_profile(source_id: str, *, catalog_root: Path | None = None) -> SourceProfile:
    root = catalog_root or CATALOG_ROOT
    return SourceProfile.model_validate(load_yaml(root / "sources" / f"{source_id}.yaml"))


def load_mapping_spec(path: str | Path) -> MappingSpec:
    return MappingSpec.model_validate(load_yaml(path))


def load_route_set(path: str | Path) -> RouteSet:
    return RouteSet.model_validate(load_yaml(path))


def load_quality_rules(paths: list[str | Path]) -> QualityRuleSet:
    rules = []
    for path in paths:
        rules.extend(QualityRuleSet.model_validate(load_yaml(path)).rules)
    return QualityRuleSet(rules=rules)


def load_metric_definition(path: str | Path) -> MetricDefinition:
    return MetricDefinition.model_validate(load_yaml(path))


def load_workflow_definition(path: str | Path) -> WorkflowDefinition:
    return WorkflowDefinition.model_validate(load_yaml(path))
