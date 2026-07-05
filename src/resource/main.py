import asyncio
from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel, Field


class ResourceEvent(BaseModel):
    node_id: int
    request_id: str
    logical_time: int = Field(ge=0)


class ResourceState:
    def __init__(self) -> None:
        self.current: dict[str, int] = {}
        self.events: list[dict[str, object]] = []
        self.violations = 0
        self._lock = asyncio.Lock()

    async def reset(self) -> None:
        async with self._lock:
            self.current = {}
            self.events = []
            self.violations = 0

    async def enter(self, event: ResourceEvent) -> dict[str, object]:
        async with self._lock:
            overlap = bool(self.current)
            if overlap:
                self.violations += 1
            self.current[event.request_id] = event.node_id
            record = self._record("ENTER", event, overlap)
            self.events.append(record)
            return dict(record)

    async def exit(self, event: ResourceEvent) -> dict[str, object]:
        async with self._lock:
            self.current.pop(event.request_id, None)
            record = self._record("EXIT", event, False)
            self.events.append(record)
            return dict(record)

    async def snapshot(self) -> dict[str, object]:
        async with self._lock:
            return {
                "current": dict(self.current),
                "violations": self.violations,
                "events_stored": len(self.events),
                "entries": sum(1 for event in self.events if event["action"] == "ENTER"),
                "exits": sum(1 for event in self.events if event["action"] == "EXIT"),
            }

    async def history(self) -> list[dict[str, object]]:
        async with self._lock:
            return [dict(event) for event in self.events]

    def _record(self, action: str, event: ResourceEvent, overlap: bool) -> dict[str, object]:
        record = {
            "wall_time": datetime.now().isoformat(timespec="milliseconds"),
            "action": action,
            "node_id": event.node_id,
            "request_id": event.request_id,
            "logical_time": event.logical_time,
            "overlap": overlap,
        }
        print(
            f"wall={record['wall_time']} | resource | {action:<5} | node={event.node_id} "
            f"| request={event.request_id} | L={event.logical_time} | overlap={overlap}",
            flush=True,
        )
        return record


app = FastAPI(title="MC714 resource observer")
state = ResourceState()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/enter")
async def enter(event: ResourceEvent) -> dict[str, object]:
    return await state.enter(event)


@app.post("/exit")
async def exit_resource(event: ResourceEvent) -> dict[str, object]:
    return await state.exit(event)


@app.post("/reset")
async def reset() -> dict[str, str]:
    await state.reset()
    return {"status": "reset"}


@app.get("/state")
async def get_state() -> dict[str, object]:
    return await state.snapshot()


@app.get("/events")
async def get_events() -> list[dict[str, object]]:
    return await state.history()
