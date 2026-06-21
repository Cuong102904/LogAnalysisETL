from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class FactBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_time: datetime
    actor_id: str | None = None
    session_id: str | None = None
    course_id: str | None = None
    org_id: str | None = None

    def as_record(self) -> dict[str, Any]:
        return self.model_dump(mode="python")


class AssessmentEvent(FactBase):
    problem_id: str | None = None
    assessment_action: str
    problem_display_name: str | None = None
    response_type: str | None = None
    input_type: str | None = None
    attempts: int | None = None
    success: str | None = None
    grade: float | None = None
    max_grade: float | None = None
    weighted_earned: float | None = None
    weighted_possible: float | None = None
    answers_json: dict[str, Any] | None = None
    correct_map_json: dict[str, Any] | None = None
    submission_json: dict[str, Any] | None = None
    event_transaction_id: str | None = None


class VideoEvent(FactBase):
    video_id: str | None = None
    video_action: str
    video_code: str | None = None
    duration_seconds: float | None = None
    current_time_seconds: float | None = None
    old_time_seconds: float | None = None
    new_time_seconds: float | None = None
    old_speed: float | None = None
    new_speed: float | None = None
    saved_position: str | None = None
    transcript_language: str | None = None
    completion_status: str | None = None


class DocumentEvent(FactBase):
    document_action: str
    document_type: str | None = None
    document_id: str | None = None
    asset_url: str | None = None
    file_name: str | None = None
    chapter: str | None = None
    chapter_title: str | None = None
    page_number: int | None = None
    old_value: str | None = None
    new_value: str | None = None
    zoom_amount: float | None = None
    scroll_direction: str | None = None
    search_query: str | None = None
    search_status: str | None = None
    case_sensitive: bool | None = None
    highlight_all: bool | None = None


class NavigationEvent(FactBase):
    navigation_action: str
    current_url: str | None = None
    target_url: str | None = None
    current_tab: int | None = None
    target_tab: int | None = None
    tab_count: int | None = None
    widget_placement: str | None = None
    displayed_in: str | None = None


class ExamEvent(FactBase):
    exam_action: str
    exam_id: int | None = None
    exam_content_id: str | None = None
    exam_name: str | None = None
    exam_default_time_limit_mins: int | None = None
    exam_is_proctored: bool | None = None
    exam_is_practice_exam: bool | None = None
    exam_is_active: bool | None = None
    attempt_id: int | None = None
    attempt_user_id: int | None = None
    attempt_started_at: datetime | None = None
    attempt_completed_at: datetime | None = None
    attempt_status: str | None = None
    attempt_elapsed_time_secs: float | None = None
    quiz_nav_action: str | None = None


class CourseContentEvent(FactBase):
    content_action: str
    content_type: str | None = None
    block_type: str | None = None
    block_id: str | None = None
    content_url: str | None = None
    target_block_id: str | None = None
    completion_value: int | None = None


class AuthoringEvent(FactBase):
    authoring_action: str
    library_key: str | None = None
    xblock_usage_key: str | None = None
    xblock_type: str | None = None
    container_id: str | None = None
    grading_policy_hash: str | None = None
    request_get_json: dict[str, Any] | None = None
    request_post_json: dict[str, Any] | None = None


class AuthEvent(FactBase):
    auth_action: str
    provider: str | None = None
    next_url: str | None = None
    registration_status: str | None = None
    has_oauth_code: bool | None = None


class SystemEvent(FactBase):
    system_action: str
    noise_type: str | None = None
    path: str | None = None
    reason: str | None = None


class UnknownEvent(FactBase):
    unknown_action: str = "unknown"
    raw_name: str | None = None
    raw_event_type: str | None = None


FACT_MODEL_BY_TARGET: dict[str, type[FactBase]] = {
    "silver_assessment_events": AssessmentEvent,
    "silver_video_events": VideoEvent,
    "silver_document_events": DocumentEvent,
    "silver_navigation_events": NavigationEvent,
    "silver_exam_events": ExamEvent,
    "silver_course_content_events": CourseContentEvent,
    "silver_authoring_events": AuthoringEvent,
    "silver_auth_events": AuthEvent,
    "silver_system_events": SystemEvent,
    "silver_unknown_events": UnknownEvent,
}

