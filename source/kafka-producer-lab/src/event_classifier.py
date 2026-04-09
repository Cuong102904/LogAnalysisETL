"""Classification rules for LMS exam-day tracking logs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ClassificationResult:
    """Result of routing decision for one raw event."""

    label: str
    reason: str


VIDEO_EVENT_TYPES = {
    "play_video",
    "pause_video",
    "seek_video",
    "speed_change_video",
    "completion",
}

EXAM_EVENT_TYPES = {
    "problem_check",
    "edx.grades.problem.submitted",
}

NOISE_PATH_TOKENS = (
    "wp-login",
    "wp-json",
    "xmlrpc",
    "/admin/",
    "/.env",
    "/.git/config",
    "/robots.txt",
)

BOT_UA_TOKENS = (
    "bot",
    "spider",
    "crawler",
    "scanner",
    "semrush",
    "bingbot",
    "googlebot",
    "applebot",
)


def _safe_lower(value: Any) -> str:
    return str(value or "").lower()


def classify_event(record: dict[str, Any]) -> ClassificationResult:
    """Classify event into exam, learning, noise, or other."""
    event_type = _safe_lower(record.get("event_type"))
    name = _safe_lower(record.get("name"))
    event_source = _safe_lower(record.get("event_source"))
    username = _safe_lower(record.get("username"))
    agent = _safe_lower(record.get("agent"))
    page = _safe_lower(record.get("page"))
    referer = _safe_lower(record.get("referer"))
    context = record.get("context") if isinstance(record.get("context"), dict) else {}
    path = _safe_lower(context.get("path"))
    course_id = _safe_lower(context.get("course_id"))

    joined_text = " ".join([event_type, name, page, referer, path, course_id])

    if event_type.startswith("edx.special_exam.timed.attempt."):
        return ClassificationResult("exam", "special_exam_event")

    if "edx_proctoring" in joined_text or "proctored_exam" in joined_text:
        return ClassificationResult("exam", "proctoring_marker")

    exam_context_hit = "finalexam" in joined_text or "in_exam" in joined_text
    if event_type in EXAM_EVENT_TYPES and exam_context_hit:
        return ClassificationResult("exam", "graded_exam_context")

    if event_type in VIDEO_EVENT_TYPES or event_type.startswith("edx.ui.lms.sequence."):
        return ClassificationResult("learning", "learning_event_type")

    if event_type in EXAM_EVENT_TYPES:
        return ClassificationResult("learning", "assessment_non_exam_context")

    if any(token in joined_text for token in NOISE_PATH_TOKENS):
        return ClassificationResult("noise", "known_noise_path")

    if event_source == "server" and not username and any(token in agent for token in BOT_UA_TOKENS):
        return ClassificationResult("noise", "bot_signature")

    return ClassificationResult("other", "fallback")
