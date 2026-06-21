from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from learnlake.plugins import TransformRegistry

_BLOCK_RE = re.compile(r"\+block@([^/?]+)")
_BLOCK_TYPE_RE = re.compile(r"\+type@([^+]+)\+block@")
_COURSE_RE = re.compile(r"course-v1:[^/?#]+")
_PROVIDER_RE = re.compile(r"/auth/(?:login|complete)/([^/]+)/?")
_BOT_RE = re.compile(r"bot|crawler|spider|curl|wget|prefetch", re.IGNORECASE)
_NOISE_PATH_RE = re.compile(r"^/(robots\.txt|wp-login\.php|xmlrpc\.php|\.git|\.env)")


def _text(value: Any) -> str:
    return str(value or "")


def _base_fact(event_index: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": event_index["event_id"],
        "event_time": event_index["event_time"],
        "actor_id": event_index.get("actor_id"),
        "session_id": event_index.get("session_id"),
        "course_id": event_index.get("course_id"),
        "org_id": event_index.get("org_id"),
    }


def _coerce_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_list_value(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _parsed_request_payload(payload: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}, {}
    get_data = payload.get("GET") if isinstance(payload.get("GET"), dict) else {}
    post_data = payload.get("POST") if isinstance(payload.get("POST"), dict) else {}
    return get_data, post_data


def _extract_block_type(path: Any, event_type: Any) -> str | None:
    for value in (path, event_type):
        match = _BLOCK_TYPE_RE.search(_text(value))
        if match:
            return match.group(1)
    return None


def extract_object_id(path: Any, event_type: Any) -> str | None:
    text = _text(path)
    match = _BLOCK_RE.search(text)
    if match:
        return match.group(1)
    if event_type:
        event_match = _BLOCK_RE.search(_text(event_type))
        if event_match:
            return event_match.group(1)
        return str(event_type)
    return None


def extract_course_id(path: Any, event_type: Any) -> str | None:
    for value in (path, event_type):
        match = _COURSE_RE.search(_text(value))
        if match:
            return match.group(0)
    return None


def is_authenticated(username: Any, user_id: Any) -> bool:
    return bool(username or user_id)


def is_bot(agent: Any, username: Any, event_type: Any) -> bool:
    if agent and _BOT_RE.search(str(agent)):
        return True
    if username and str(username).lower() in {"anonymous", "none"}:
        return True
    text = _text(event_type)
    return text.startswith("/heartbeat") or _NOISE_PATH_RE.search(text) is not None


def payload_json_dict(payload: Any) -> dict[str, Any] | None:
    return payload if isinstance(payload, dict) else None


def _provider_from_path(raw_event_type: Any, path: Any) -> str | None:
    for value in (raw_event_type, path):
        match = _PROVIDER_RE.search(_text(value))
        if match:
            return match.group(1)
    return None


def _file_name_from_url(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value)
    candidate = parsed.path.rsplit("/", 1)[-1]
    return candidate or None


def _value_at_path(payload: Any, key: str) -> Any:
    if not isinstance(payload, dict):
        return None
    return payload.get(key)


def extract_assessment_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    record = _base_fact(event_index)
    raw_payload = bronze.get("raw_payload") or {}
    context = raw_payload.get("context") or {}
    module = context.get("module") if isinstance(context, dict) else {}
    get_data, post_data = _parsed_request_payload(payload)
    problem_id = extract_object_id(context.get("path"), raw_payload.get("event_type"))
    response_type = input_type = None
    answers = correct_map = submission = None
    attempts = success = None
    grade = max_grade = weighted_earned = weighted_possible = None
    event_transaction_id = None

    if isinstance(payload, dict):
        problem_id = payload.get("problem_id") or problem_id
        response_type = _value_at_path(payload.get("submission"), "response_type")
        input_type = _value_at_path(payload.get("submission"), "input_type")
        answers = payload.get("answers")
        correct_map = payload.get("correct_map")
        submission = payload.get("submission")
        attempts = _coerce_int(payload.get("attempts"))
        success = payload.get("success")
        grade = _coerce_float(payload.get("grade"))
        max_grade = _coerce_float(payload.get("max_grade"))
        weighted_earned = _coerce_float(payload.get("weighted_earned"))
        weighted_possible = _coerce_float(payload.get("weighted_possible"))
        event_transaction_id = payload.get("event_transaction_id")
    elif isinstance(payload, list):
        submission = {"raw_items": payload[:2]}
    elif isinstance(payload, str):
        submission = parse_qs(payload, keep_blank_values=True)

    if not answers and post_data:
        answers = {key: _first_list_value(value) for key, value in post_data.items()}

    record.update(
        {
            "problem_id": problem_id,
            "assessment_action": route["action"],
            "problem_display_name": module.get("display_name") if isinstance(module, dict) else None,
            "response_type": response_type,
            "input_type": input_type,
            "attempts": attempts,
            "success": success,
            "grade": grade,
            "max_grade": max_grade,
            "weighted_earned": weighted_earned,
            "weighted_possible": weighted_possible,
            "answers_json": answers,
            "correct_map_json": correct_map,
            "submission_json": submission,
            "event_transaction_id": event_transaction_id,
        }
    )
    return [{"target_table": "silver_assessment_events", "record": record}]


def extract_video_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    record = _base_fact(event_index)
    raw_payload = bronze.get("raw_payload") or {}
    raw_event_type = _text(raw_payload.get("event_type") or raw_payload.get("name"))
    context = raw_payload.get("context") or {}
    _, post_data = _parsed_request_payload(payload)
    path = context.get("path") if isinstance(context, dict) else None
    video_id = None
    if isinstance(payload, dict):
        video_id = payload.get("id")
    if not video_id:
        video_id = extract_object_id(path, raw_payload.get("event_type"))
    transcript_language = None
    if isinstance(path, str) and "/handler/transcript/" in path:
        transcript_language = path.rsplit("/", 1)[-1]
    video_action = route["action"]
    if raw_event_type in {
        "load_video",
        "play_video",
        "pause_video",
        "stop_video",
        "seek_video",
        "speed_change_video",
    }:
        video_action = raw_event_type.removesuffix("_video")
    record.update(
        {
            "video_id": video_id,
            "video_action": video_action,
            "video_code": payload.get("code") if isinstance(payload, dict) else None,
            "duration_seconds": _coerce_float(payload.get("duration")) if isinstance(payload, dict) else None,
            "current_time_seconds": _coerce_float(
                payload.get("currentTime") if isinstance(payload, dict) else None
            ),
            "old_time_seconds": _coerce_float(payload.get("old_time")) if isinstance(payload, dict) else None,
            "new_time_seconds": _coerce_float(payload.get("new_time")) if isinstance(payload, dict) else None,
            "old_speed": _coerce_float(payload.get("old_speed")) if isinstance(payload, dict) else None,
            "new_speed": _coerce_float(payload.get("new_speed")) if isinstance(payload, dict) else None,
            "saved_position": _first_list_value(post_data.get("saved_video_position")),
            "transcript_language": transcript_language,
            "completion_status": _first_list_value(post_data.get("result")),
        }
    )
    return [{"target_table": "silver_video_events", "record": record}]


def extract_video_completion_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    facts = extract_video_event(bronze, event_index, payload, route)
    content_record = _base_fact(event_index)
    context = (bronze.get("raw_payload") or {}).get("context") or {}
    path = context.get("path") if isinstance(context, dict) else None
    content_record.update(
        {
            "content_action": "complete",
            "content_type": "video",
            "block_type": _extract_block_type(path, (bronze.get("raw_payload") or {}).get("event_type")),
            "block_id": extract_object_id(path, (bronze.get("raw_payload") or {}).get("event_type")),
            "content_url": path,
            "target_block_id": extract_object_id(path, (bronze.get("raw_payload") or {}).get("event_type")),
            "completion_value": _coerce_int(_first_list_value((_parsed_request_payload(payload)[1]).get("completion"))),
        }
    )
    facts.append({"target_table": "silver_course_content_events", "record": content_record})
    return facts


def extract_document_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    record = _base_fact(event_index)
    raw_payload = bronze.get("raw_payload") or {}
    context = raw_payload.get("context") or {}
    page = raw_payload.get("page")
    document_action = route["action"]
    if raw_payload.get("event_type") == "book":
        document_action = "view"
    elif raw_payload.get("event_type") == "textbook.pdf.page.scrolled":
        document_action = "scroll"
    elif raw_payload.get("event_type") == "textbook.pdf.display.scaled":
        document_action = "zoom"
    elif raw_payload.get("event_type") == "textbook.pdf.search.executed":
        document_action = "search"
    asset_url = page if isinstance(page, str) and "file=" in page else raw_payload.get("referer")
    if isinstance(page, str) and "file=" in page:
        file_fragment = page.split("file=", 1)[1].split("#", 1)[0]
        asset_url = file_fragment
    record.update(
        {
            "document_action": document_action,
            "document_type": "pdf" if "pdfbook" in _text(context.get("path")) else "document",
            "document_id": extract_object_id(context.get("path"), raw_payload.get("event_type")),
            "asset_url": asset_url,
            "file_name": _file_name_from_url(asset_url),
            "chapter": payload.get("chapter") if isinstance(payload, dict) else None,
            "chapter_title": payload.get("chapter_title") if isinstance(payload, dict) else None,
            "page_number": _coerce_int(payload.get("page")) if isinstance(payload, dict) else None,
            "old_value": None if not isinstance(payload, dict) else _text(payload.get("old")) or None,
            "new_value": None if not isinstance(payload, dict) else _text(payload.get("new")) or None,
            "zoom_amount": _coerce_float(payload.get("amount")) if isinstance(payload, dict) else None,
            "scroll_direction": payload.get("direction") if isinstance(payload, dict) else None,
            "search_query": payload.get("query") if isinstance(payload, dict) else None,
            "search_status": payload.get("status") if isinstance(payload, dict) else None,
            "case_sensitive": bool(payload.get("caseSensitive")) if isinstance(payload, dict) and payload.get("caseSensitive") is not None else None,
            "highlight_all": bool(payload.get("highlightAll")) if isinstance(payload, dict) and payload.get("highlightAll") is not None else None,
        }
    )
    return [{"target_table": "silver_document_events", "record": record}]


def extract_navigation_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_event_type = _text((bronze.get("raw_payload") or {}).get("event_type"))
    navigation_action = "navigate"
    if raw_event_type in {"seq_next", "seq_prev"}:
        navigation_action = "sequence_next" if raw_event_type == "seq_next" else "sequence_previous"
    elif raw_event_type.endswith("link_clicked"):
        navigation_action = "link_clicked"
    elif raw_event_type.endswith("next_selected"):
        navigation_action = "sequence_next"
    elif raw_event_type.endswith("previous_selected"):
        navigation_action = "sequence_previous"
    elif raw_event_type.endswith("tab_selected"):
        navigation_action = "sequence_tab"
    elif raw_event_type.endswith("resume_course.clicked"):
        navigation_action = "resume_course"
    elif "sidebarupsell" in raw_event_type:
        navigation_action = "display"
    record = _base_fact(event_index)
    record.update(
        {
            "navigation_action": navigation_action,
            "current_url": payload.get("current_url") if isinstance(payload, dict) else None,
            "target_url": payload.get("target_url") if isinstance(payload, dict) else None,
            "current_tab": _coerce_int(payload.get("current_tab")) if isinstance(payload, dict) else None,
            "target_tab": _coerce_int(payload.get("target_tab")) if isinstance(payload, dict) else None,
            "tab_count": _coerce_int(payload.get("tab_count")) if isinstance(payload, dict) else None,
            "widget_placement": payload.get("widget_placement") if isinstance(payload, dict) else None,
            "displayed_in": payload.get("displayed_in") if isinstance(payload, dict) else None,
        }
    )
    return [{"target_table": "silver_navigation_events", "record": record}]


def extract_exam_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_payload = bronze.get("raw_payload") or {}
    path = ((raw_payload.get("context") or {}) if isinstance(raw_payload.get("context"), dict) else {}).get("path")
    quiz_nav_action = None
    if isinstance(path, str) and "quiz_navigation" in path:
        quiz_nav_action = path.rstrip("/").rsplit("/", 1)[-1]
    record = _base_fact(event_index)
    record.update(
        {
            "exam_action": route["action"],
            "exam_id": _coerce_int(payload.get("exam_id")) if isinstance(payload, dict) else None,
            "exam_content_id": payload.get("exam_content_id") if isinstance(payload, dict) else None,
            "exam_name": payload.get("exam_name") if isinstance(payload, dict) else None,
            "exam_default_time_limit_mins": _coerce_int(payload.get("exam_default_time_limit_mins")) if isinstance(payload, dict) else None,
            "exam_is_proctored": payload.get("exam_is_proctored") if isinstance(payload, dict) else None,
            "exam_is_practice_exam": payload.get("exam_is_practice_exam") if isinstance(payload, dict) else None,
            "exam_is_active": payload.get("exam_is_active") if isinstance(payload, dict) else None,
            "attempt_id": _coerce_int(payload.get("attempt_id")) if isinstance(payload, dict) else None,
            "attempt_user_id": _coerce_int(payload.get("attempt_user_id")) if isinstance(payload, dict) else None,
            "attempt_started_at": payload.get("attempt_started_at") if isinstance(payload, dict) else None,
            "attempt_completed_at": payload.get("attempt_completed_at") if isinstance(payload, dict) else None,
            "attempt_status": payload.get("attempt_status") if isinstance(payload, dict) else None,
            "attempt_elapsed_time_secs": _coerce_float(payload.get("attempt_event_elapsed_time_secs")) if isinstance(payload, dict) else None,
            "quiz_nav_action": quiz_nav_action,
        }
    )
    return [{"target_table": "silver_exam_events", "record": record}]


def extract_course_content_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_payload = bronze.get("raw_payload") or {}
    context = raw_payload.get("context") or {}
    path = context.get("path") if isinstance(context, dict) else None
    event_type = raw_payload.get("event_type")
    content_action = "view"
    if isinstance(path, str) and "/jump_to/" in path:
        content_action = "jump_to"
    elif isinstance(path, str) and path.endswith("/course/"):
        content_action = "course_home"
    elif isinstance(path, str) and path.endswith("/progress"):
        content_action = "progress"
    elif isinstance(path, str) and "/about" in path:
        content_action = "about"
    elif isinstance(path, str) and path.endswith("/cohorts/"):
        content_action = "cohorts"
    elif isinstance(path, str) and "/courseware/" in path:
        content_action = "courseware_page"
    record = _base_fact(event_index)
    record.update(
        {
            "content_action": content_action,
            "content_type": "course_content",
            "block_type": _extract_block_type(path, event_type),
            "block_id": extract_object_id(path, event_type),
            "content_url": path,
            "target_block_id": extract_object_id(path, event_type),
            "completion_value": None,
        }
    )
    return [{"target_table": "silver_course_content_events", "record": record}]


def extract_authoring_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_payload = bronze.get("raw_payload") or {}
    context = raw_payload.get("context") or {}
    path = context.get("path") if isinstance(context, dict) else None
    get_data, post_data = _parsed_request_payload(payload)
    authoring_action = "author"
    if isinstance(path, str) and "container_preview" in path:
        authoring_action = "container_preview"
    elif isinstance(path, str) and "author_view" in path:
        authoring_action = "author_view"
    elif isinstance(path, str) and "studio_view" in path:
        authoring_action = "studio_view"
    elif isinstance(path, str) and "library" in path:
        authoring_action = "library_view"
    record = _base_fact(event_index)
    record.update(
        {
            "authoring_action": authoring_action,
            "library_key": extract_course_id(path, raw_payload.get("event_type")),
            "xblock_usage_key": _text(path or raw_payload.get("event_type")) or None,
            "xblock_type": _extract_block_type(path, raw_payload.get("event_type")),
            "container_id": extract_object_id(path, raw_payload.get("event_type")),
            "grading_policy_hash": payload.get("grading_policy_hash") if isinstance(payload, dict) else None,
            "request_get_json": get_data or None,
            "request_post_json": post_data or None,
        }
    )
    return [{"target_table": "silver_authoring_events", "record": record}]


def extract_auth_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_payload = bronze.get("raw_payload") or {}
    context = raw_payload.get("context") or {}
    path = context.get("path") if isinstance(context, dict) else None
    get_data, _ = _parsed_request_payload(payload)
    raw_event_type = raw_payload.get("event_type")
    auth_action = "authenticate"
    if isinstance(path, str) and "/register" in path:
        auth_action = "register"
    elif isinstance(path, str) and "/complete/" in path:
        auth_action = "oauth_complete"
    elif isinstance(path, str) and "/login/" in path:
        auth_action = "login_start"
    record = _base_fact(event_index)
    record.update(
        {
            "auth_action": auth_action,
            "provider": _provider_from_path(raw_event_type, path),
            "next_url": _first_list_value(get_data.get("next")),
            "registration_status": None,
            "has_oauth_code": bool(_first_list_value(get_data.get("code"))),
        }
    )
    return [{"target_table": "silver_auth_events", "record": record}]


def extract_system_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_payload = bronze.get("raw_payload") or {}
    context = raw_payload.get("context") or {}
    path = context.get("path") if isinstance(context, dict) else raw_payload.get("event_type")
    reason = "noise" if event_index.get("is_noise") else "system"
    record = _base_fact(event_index)
    record.update(
        {
            "system_action": route["action"],
            "noise_type": "scanner" if event_index.get("is_noise") else None,
            "path": path,
            "reason": reason,
        }
    )
    return [{"target_table": "silver_system_events", "record": record}]


def extract_unknown_event(
    bronze: dict[str, Any],
    event_index: dict[str, Any],
    payload: Any,
    route: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_payload = bronze.get("raw_payload") or {}
    record = _base_fact(event_index)
    record.update(
        {
            "unknown_action": "unknown",
            "raw_name": raw_payload.get("name"),
            "raw_event_type": raw_payload.get("event_type"),
        }
    )
    return [{"target_table": "silver_unknown_events", "record": record}]


def register_transforms(registry: TransformRegistry) -> TransformRegistry:
    registry.register("daotao_extract_object_id", extract_object_id)
    registry.register("daotao_extract_course_id", extract_course_id)
    registry.register("daotao_is_authenticated", is_authenticated)
    registry.register("daotao_is_bot", is_bot)
    registry.register("daotao_payload_json_dict", payload_json_dict)
    registry.register("daotao_assessment_event", extract_assessment_event)
    registry.register("daotao_video_event", extract_video_event)
    registry.register("daotao_video_completion_event", extract_video_completion_event)
    registry.register("daotao_document_event", extract_document_event)
    registry.register("daotao_navigation_event", extract_navigation_event)
    registry.register("daotao_exam_event", extract_exam_event)
    registry.register("daotao_course_content_event", extract_course_content_event)
    registry.register("daotao_authoring_event", extract_authoring_event)
    registry.register("daotao_auth_event", extract_auth_event)
    registry.register("daotao_system_event", extract_system_event)
    registry.register("daotao_unknown_event", extract_unknown_event)
    return registry
