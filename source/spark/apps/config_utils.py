from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from utils.config_loader import load_yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_app_config(env_name: str, file_name: str) -> dict[str, Any]:
    config_path = os.getenv(env_name)
    path = Path(config_path) if config_path else PROJECT_ROOT / "configs" / "app" / file_name
    return load_yaml(str(path))


def nested_value(config: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def env_str(env_name: str, config: dict[str, Any], *keys: str, default: str) -> str:
    value = os.getenv(env_name)
    if value is not None:
        return value
    return str(nested_value(config, *keys, default=default))


def required_env_str(env_name: str, config: dict[str, Any], *keys: str) -> str:
    value = os.getenv(env_name)
    if value is not None:
        return value
    config_value = nested_value(config, *keys)
    if config_value is None:
        config_key = ".".join(keys)
        raise ValueError(f"Missing required config {config_key} or env {env_name}")
    return str(config_value)


def env_int(env_name: str, config: dict[str, Any], *keys: str, default: int) -> int:
    value = os.getenv(env_name)
    if value is not None:
        return int(value)
    return int(nested_value(config, *keys, default=default))
