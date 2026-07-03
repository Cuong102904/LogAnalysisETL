from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class FactBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def as_record(self) -> dict[str, object]:
        return self.model_dump(mode="python")


class ProblemSubmission(FactBase):
    submission_event_id: str
    event_time_utc: datetime
    username: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    course_id: str | None = None
    problem_id: str | None = None
    module_usage_key: str | None = None
    module_display_name: str | None = None
    submission_source: str
    answer_payload: str | None = None
    attempt_no: int | None = None
    success: str | None = None
    grade_raw: float | None = None
    max_grade_raw: float | None = None
    question_variant: str | None = None
    in_exam_window: bool = False
    exam_attempt_id: str | None = None


class ProblemGrade(FactBase):
    grade_event_id: str
    event_time_utc: datetime
    username: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    course_id: str | None = None
    problem_id: str | None = None
    module_usage_key: str | None = None
    module_display_name: str | None = None
    weighted_earned: float | None = None
    weighted_possible: float | None = None
    is_correct: bool | None = None
    grade_ratio: float | None = None
    exam_attempt_id: str | None = None
    in_exam_window: bool = False


class ExamAttempt(FactBase):
    exam_attempt_event_id: str
    event_time_utc: datetime
    attempt_event_type: str
    exam_attempt_id: str
    exam_id: str | None = None
    exam_name: str | None = None
    exam_content_id: str | None = None
    course_id: str | None = None
    username: str | None = None
    user_id: str | None = None
    attempt_user_id: str | None = None
    session_id: str | None = None
    attempt_code: str | None = None
    created_time_utc: datetime | None = None
    started_time_utc: datetime | None = None
    submitted_time_utc: datetime | None = None
    attempt_event_elapsed_time_secs: float | None = None
    allowed_time_limit_mins: int | None = None
    exam_default_time_limit_mins: int | None = None
    attempt_status: str | None = None
    exam_is_active: bool | None = None
    is_proctored: bool | None = None
    is_practice_exam: bool | None = None


class VideoInteraction(FactBase):
    video_event_id: str
    event_time_utc: datetime
    username: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    course_id: str | None = None
    video_id: str
    video_block_id: str | None = None
    video_code: str | None = None
    action_type: str
    duration_s: float | None = None
    current_time_s: float | None = None
    old_time_s: float | None = None
    new_time_s: float | None = None
    old_speed: float | None = None
    new_speed: float | None = None
    speed: float | None = None
    seek_type: str | None = None
    watch_ratio: float | None = None
    position_bucket_10s: int | None = None
    position_bucket_30s: int | None = None


class NavigationEvent(FactBase):
    navigation_event_id: str
    event_time_utc: datetime
    username: str | None = None
    session_id: str | None = None
    course_id: str | None = None
    nav_type: str
    nav_name: str | None = None
    from_block: str | None = None
    to_block: str | None = None
    old_tab: int | None = None
    new_tab: int | None = None
    target_tab: int | None = None
    current_tab: int | None = None
    tab_count: int | None = None
    widget_placement: str | None = None
    position: int | None = None
    usage_key: str | None = None
    page: str | None = None
    referer: str | None = None


class ContentAccessEvent(FactBase):
    event_time_utc: datetime
    username: str | None = None
    session_id: str | None = None
    course_id: str | None = None
    content_type: str
    content_event_name: str | None = None
    chapter: str | None = None
    page_no: int | None = None
    direction: str | None = None
    old_page: int | None = None
    new_page: int | None = None
    zoom_amount: float | None = None


class SystemNoiseEvent(FactBase):
    event_time_utc: datetime
    host: str | None = None
    ip: str | None = None
    agent: str | None = None
    event_type: str | None = None
    context_path: str | None = None
    noise_family: str
    username_empty_flag: bool = False


class UnknownEvent(FactBase):
    event_id: str
    event_time_utc: datetime | None = None
    event_date: date | None = None
    username: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    course_id: str | None = None
    event_source: str | None = None
    event_type: str | None = None
    event_name: str | None = None
    context_path: str | None = None
    unknown_reason: str
    route_version: str
    replay_status: str
    first_seen_at: datetime
    last_replayed_at: datetime | None = None
    resolved_at: datetime | None = None
    resolved_route_id: str | None = None
    raw_json: str


FACT_MODEL_BY_TARGET: dict[str, type[FactBase]] = {
    "problem_submissions": ProblemSubmission,
    "problem_grades": ProblemGrade,
    "exam_attempts": ExamAttempt,
    "video_interactions": VideoInteraction,
    "navigation_events": NavigationEvent,
    "content_access_events": ContentAccessEvent,
    "system_noise_events": SystemNoiseEvent,
    "silver_unknown_events": UnknownEvent,
}
