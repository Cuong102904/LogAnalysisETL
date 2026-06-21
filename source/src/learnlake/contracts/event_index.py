from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

LearningRelevance = Literal["learning", "non_learning", "noise", "unknown"]
QualityStatus = Literal["valid", "warning", "invalid", "ignored"]


class EventIndex(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    source_id: str
    source_type: str
    raw_event_ref: str
    event_time: datetime
    ingest_time: datetime | None = None
    processing_time: datetime

    actor_id: str | None = None
    actor_external_id: str | None = None
    session_id: str | None = None
    course_id: str | None = None
    org_id: str | None = None

    raw_name: str | None = None
    raw_event_type: str
    event_source: str | None = None
    event_group: str
    normalized_type: str
    action: str
    object_type: str
    object_id: str | None = None
    learning_relevance: LearningRelevance

    path: str | None = None
    page: str | None = None
    referer: str | None = None
    host: str | None = None
    ip: str | None = None
    user_agent: str | None = None

    is_authenticated: bool
    is_bot: bool
    is_noise: bool = False
    quality_status: QualityStatus = "valid"
    quality_errors: list[str] = Field(default_factory=list)

    payload_kind: str
    payload_json: dict[str, Any] | None = None
    context: dict[str, Any] | None = None

    event_date: date | None = None
    event_hour: int | None = None

    def as_record(self) -> dict[str, Any]:
        return self.model_dump(mode="python")

