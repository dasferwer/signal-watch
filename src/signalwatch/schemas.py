from ipaddress import ip_address
from time import time
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    occurred_at: float = Field(ge=0, le=4102444800, allow_inf_nan=False)
    actor_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,80}$")
    source_ip: str = Field(max_length=45)
    country: str = Field(pattern=r"^[A-Z]{2}$")
    kind: Literal["api", "login", "signup"]
    success: bool = Field(strict=True)
    bytes_sent: int = Field(default=0, ge=0, le=1_000_000_000, strict=True)

    @field_validator("source_ip")
    @classmethod
    def normalize_ip(cls, value):
        return str(ip_address(value))

    @field_validator("occurred_at")
    @classmethod
    def reject_future(cls, value):
        if value > time() + 30:
            raise ValueError("Event time is more than 30 seconds ahead of the server clock")
        return value


class EventBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    events: list[Event] = Field(min_length=1, max_length=200)


class Review(BaseModel):
    model_config = ConfigDict(extra="forbid")
    verdict: Literal["confirmed", "false_positive", "needs_context"]
    note: str = Field(min_length=3, max_length=500)
