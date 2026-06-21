from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BronzeEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    source_id: str
    source_type: str
    source_event_type: str | None = None
    event_time_raw: str | None = None
    event_time: datetime | None = None
    ingestion_time: datetime
    raw_payload: dict[str, Any] | str
    kafka_topic: str | None = None
    kafka_partition: int | None = None
    kafka_offset: int | None = None
    schema_version: str = Field(default="bronze-envelope-v1")
    processing_date: date

    def as_record(self) -> dict[str, Any]:
        return self.model_dump(mode="python")
