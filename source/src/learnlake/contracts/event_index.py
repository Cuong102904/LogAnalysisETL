from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class EventsCanonical(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_time_utc: datetime
    event_date: date
    event_hour_utc: int
    username: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    ip: str | None = None
    agent: str | None = None
    host: str | None = None
    page: str | None = None
    referer: str | None = None
    event_source: str | None = None
    event_type: str
    event_name: str | None = None
    course_id: str | None = None
    org_id: str | None = None
    context_path: str | None = None
    module_usage_key: str | None = None
    module_display_name: str | None = None
    event_json: str | None = None
    event_group: str
    event_subgroup: str
    parser_family: str
    is_noise: bool = False
    is_authenticated: bool = False
    route_id: str | None = None
    route_version: str | None = None

    def as_record(self) -> dict[str, object]:
        return self.model_dump(mode="python")


EventIndex = EventsCanonical
