from __future__ import annotations

from typing import Any


def _non_blank_text(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip().lower()
    return bool(s) and s not in ("none", "null", "")


def event_has_subject_identity(event: dict[str, Any]) -> bool:
    """True when the row is tied to a known learner/session subject (username or LMS user id)."""

    if _non_blank_text(event.get("username")):
        return True

    ctx = event.get("context")
    uid = ctx.get("user_id") if isinstance(ctx, dict) else None
    if uid is not None and str(uid).strip() != "":
        try:
            n = int(str(uid).strip())
        except ValueError:
            return True
        if n <= 0:
            return False
        return True

    if _non_blank_text(event.get("user_id")):
        try:
            n = int(str(event.get("user_id")).strip())
        except (TypeError, ValueError):
            return True
        if n <= 0:
            return False
        return True

    return False
