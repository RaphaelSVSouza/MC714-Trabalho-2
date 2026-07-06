from typing import Any, Literal

from pydantic import BaseModel, Field


APP_MESSAGE = "APP_MESSAGE"
MUTEX_REQUEST = "MUTEX_REQUEST"
MUTEX_REPLY = "MUTEX_REPLY"
ELECTION = "ELECTION"
ELECTION_OK = "ELECTION_OK"
COORDINATOR = "COORDINATOR"
HEARTBEAT = "HEARTBEAT"

EventKind = Literal["lamport_event", "annotation"]


class AppMessage(BaseModel):
    type: str
    sender_id: int
    message_id: str
    logical_time: int = Field(ge=0)
    payload: dict[str, Any] = Field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        if hasattr(self, "model_dump"):
            return self.model_dump()
        return self.dict()


class SendMessageCommand(BaseModel):
    destination_id: int
    text: str


class LocalEventCommand(BaseModel):
    description: str


class CriticalSectionCommand(BaseModel):
    duration_ms: int = Field(ge=50, le=5000)


class StartElectionCommand(BaseModel):
    reason: str = "manual"


class EventRecord(BaseModel):
    wall_time: str
    logical_time: int
    node_id: int
    event_kind: EventKind
    action: str
    peer_id: int | None = None
    peer_role: str | None = None
    message_type: str | None = None
    message_id: str | None = None
    request_id: str | None = None
    request_timestamp: int | None = None
    election_id: str | None = None
    leader_id: int | None = None
    detail: str = ""
