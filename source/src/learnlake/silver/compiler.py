from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column
from pyspark.sql import functions as F

from learnlake.contracts.quality import QualityRule
from learnlake.silver.schemas import (
    RouteLeafMatch,
    RouteMatchNode,
    SilverParserConfig,
    SilverRoute,
    SilverRoutingConfig,
)


@dataclass(frozen=True)
class CompiledRoute:
    route: SilverRoute
    condition: Column


def _field_column(field: str) -> Column:
    return F.col(field)


def _compile_leaf(leaf: RouteLeafMatch) -> Column:
    column = _field_column(leaf.field)
    if leaf.op == "eq":
        return column == F.lit(leaf.value)
    if leaf.op == "in":
        return column.isin([value for value in leaf.values])
    if leaf.op == "regex":
        return column.rlike(str(leaf.value))
    if leaf.op == "contains":
        return column.contains(str(leaf.value))
    if leaf.op == "startswith":
        return column.startswith(str(leaf.value))
    if leaf.op == "exists":
        return column.isNotNull()
    raise ValueError(f"Unsupported route operator: {leaf.op}")


def compile_match_node(node: RouteMatchNode) -> Column:
    if node.leaf is not None:
        return _compile_leaf(node.leaf)
    if node.all is not None:
        condition = F.lit(True)
        for child in node.all:
            condition = condition & compile_match_node(child)
        return condition
    if node.any is not None:
        condition = F.lit(False)
        for child in node.any:
            condition = condition | compile_match_node(child)
        return condition
    if node.not_ is not None:
        return ~compile_match_node(node.not_)
    raise ValueError("Empty route match node")


def compile_routes(config: SilverRoutingConfig) -> list[CompiledRoute]:
    ordered = sorted(config.routes, key=lambda item: (-item.priority, item.id))
    return [CompiledRoute(route=route, condition=compile_match_node(route.match)) for route in ordered]


@dataclass(frozen=True)
class CompiledQualityRule:
    rule: QualityRule
    condition: Column


def _quality_field_column(field: str | None) -> Column:
    if not field:
        return F.lit(None)
    return F.col(field)


def compile_quality_rule(rule: QualityRule) -> CompiledQualityRule:
    column = _quality_field_column(rule.field)
    if rule.type == "required":
        condition = column.isNull() | (F.trim(column.cast("string")) == "")
    elif rule.type == "allowed_values":
        condition = ~column.isin(rule.allowed)
    elif rule.type == "boolean_flag":
        condition = column.cast("boolean") == F.lit(True)
    elif rule.type == "mark_when_equals":
        condition = column == F.lit(rule.value)
    elif rule.type == "unique":
        condition = F.lit(False)
    else:
        raise ValueError(f"Unsupported quality rule type: {rule.type}")
    return CompiledQualityRule(rule=rule, condition=condition)


def compile_quality_rules(rules: list[QualityRule]) -> list[CompiledQualityRule]:
    return [compile_quality_rule(rule) for rule in rules]


def parser_definitions_by_name(config: SilverParserConfig) -> dict[str, dict]:
    return {parser.name: parser.model_dump(mode="python") for parser in config.parsers}
