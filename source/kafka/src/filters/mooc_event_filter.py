from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import yaml


def load_producer_filter_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError(f"invalid filter config (expected object): {path}")
    return cfg


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _event_type(event: dict[str, Any]) -> str:
    return _safe_str(event.get("event_type")).strip()


def _event_name(event: dict[str, Any]) -> str:
    return _safe_str(event.get("name")).strip()


def _event_source(event: dict[str, Any]) -> str:
    return _safe_str(event.get("event_source")).strip()


def _context_path(event: dict[str, Any]) -> str:
    ctx = event.get("context") if isinstance(event.get("context"), dict) else {}
    return _safe_str(ctx.get("path")).strip()


def event_snapshot_for_dlq(event: dict[str, Any]) -> dict[str, str]:
    return {
        "event_type": _event_type(event),
        "event_source": _event_source(event),
        "name": _event_name(event),
        "context.path": _context_path(event),
    }


def _group_match_reason(
    *,
    group_name: str,
    group_cfg: dict[str, Any],
    event_type: str,
    event_source: str,
    event_name: str,
    context_path: str,
) -> Optional[str]:
    lower_type = event_type.lower()
    lower_source = event_source.lower()
    lower_name = event_name.lower()
    lower_path = context_path.lower()

    source_in = [str(v).lower() for v in (group_cfg.get("event_source_in") or []) if v]
    if source_in and lower_source not in source_in:
        return None

    for exact in group_cfg.get("event_type_in") or []:
        if exact and event_type == str(exact):
            return f"{group_name}: event_type in allowlist ({exact})"

    for prefix in group_cfg.get("event_type_prefixes") or []:
        prefix = str(prefix)
        if prefix and event_type.startswith(prefix):
            return f"{group_name}: event_type startswith {prefix}"

    for prefix in group_cfg.get("event_type_prefixes_seq") or []:
        prefix = str(prefix)
        if prefix and event_type.startswith(prefix):
            return f"{group_name}: event_type startswith {prefix}"

    for kw in group_cfg.get("event_type_contains_any") or []:
        kw = str(kw).lower()
        if kw and kw in lower_type:
            return f"{group_name}: event_type contains {kw}"

    conj = [str(v).lower() for v in (group_cfg.get("event_type_contains_all") or []) if v]
    if conj and all(part in lower_type for part in conj):
        return f"{group_name}: event_type contains_all {conj}"

    for kw in group_cfg.get("name_contains_any") or []:
        kw = str(kw).lower()
        if kw and kw in lower_name:
            return f"{group_name}: name contains {kw}"

    for kw in group_cfg.get("path_contains_any") or []:
        kw = str(kw).lower()
        if kw and kw in lower_path:
            return f"{group_name}: context.path contains {kw}"

    return None


def is_event_allowed(event: dict[str, Any], cfg: dict[str, Any]) -> tuple[bool, str]:
    """
    Allowlist predicate for the replay pipeline.

    Return (allowed, reason).
    """

    event_type = _event_type(event)
    event_source = _event_source(event)
    event_name = _event_name(event)
    context_path = _context_path(event)

    for group_name, group_cfg in cfg.items():
        if not isinstance(group_cfg, dict):
            continue
        reason = _group_match_reason(
            group_name=group_name,
            group_cfg=group_cfg,
            event_type=event_type,
            event_source=event_source,
            event_name=event_name,
            context_path=context_path,
        )
        if reason:
            return True, reason

    # Fallback guard: allow proctoring-ish records by name/path, but avoid FinalExam false positives.
    # Use only as a last resort; primary routing should rely on event_type patterns.
    proctor_blob = f"{event_name} {context_path}".lower()
    if "proctor" in proctor_blob and "finalexam" not in proctor_blob:
        return True, "special_exam_proctoring: name/context.path contains 'proctor' (guarded)"

    return False, f"filtered_out: event_type='{event_type}' not in allowlist"

