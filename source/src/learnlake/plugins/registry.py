from __future__ import annotations

from collections.abc import Callable
from typing import Any

Transform = Callable[..., Any]


class TransformRegistry:
    def __init__(self) -> None:
        self._transforms: dict[str, Transform] = {}

    def register(self, name: str, transform: Transform) -> None:
        if not name:
            raise ValueError("transform name is required")
        self._transforms[name] = transform

    def get(self, name: str) -> Transform:
        try:
            return self._transforms[name]
        except KeyError as exc:
            raise KeyError(f"Unregistered transform plugin: {name}") from exc

    def names(self) -> set[str]:
        return set(self._transforms)
