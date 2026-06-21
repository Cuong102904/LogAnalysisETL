from __future__ import annotations

from pathlib import Path

from learnlake.contracts import QualityRuleSet
from learnlake.runtime.config import load_yaml


def load_rule_set(path: str | Path) -> QualityRuleSet:
    return QualityRuleSet.model_validate(load_yaml(path))
