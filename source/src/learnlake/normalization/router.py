from __future__ import annotations

import re
from typing import Any

from learnlake.contracts import RouteMatch, RouteSet, RouteSpec
from learnlake.normalization.values import get_path


class RoutingError(ValueError):
    pass


def _match_has_key(value: Any, key: str) -> bool:
    if isinstance(value, dict):
        return key in value
    return False


def _evaluate_match(match: RouteMatch, record: dict[str, Any]) -> bool:
    if match.all_of is not None:
        return all(_evaluate_match(child, record) for child in match.all_of)
    if match.any_of is not None:
        return any(_evaluate_match(child, record) for child in match.any_of)
    if match.not_match is not None:
        return not _evaluate_match(match.not_match, record)

    value = get_path(record, match.field)
    if match.equals is not None:
        return value == match.equals
    if match.in_values is not None:
        return value in set(match.in_values)
    if match.starts_with is not None:
        return isinstance(value, str) and value.startswith(match.starts_with)
    if match.contains is not None:
        return isinstance(value, str) and match.contains in value
    if match.regex is not None:
        return isinstance(value, str) and re.search(match.regex, value) is not None
    if match.has_key is not None:
        return _match_has_key(value, match.has_key)
    raise RoutingError("Unsupported route match operation")


class RouteMatcher:
    def __init__(self, route_set: RouteSet) -> None:
        self.route_set = route_set
        self._routes = sorted(route_set.routes, key=lambda route: (-route.priority, route.id))

    def match(self, record: dict[str, Any]) -> RouteSpec | None:
        for route in self._routes:
            if _evaluate_match(route.match, record):
                return route
        return None

