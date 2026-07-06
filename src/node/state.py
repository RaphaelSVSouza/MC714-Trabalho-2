import asyncio
import time
from collections import deque
from datetime import datetime

from .clock import LamportClock
from .election import higher_peers, should_accept_coordinator, should_accept_election_ok, should_accept_heartbeat
from .models import APP_MESSAGE, AppMessage, EventRecord
from .mutex import HELD, RELEASED, WANTED, should_defer_reply


class MutexBusyError(Exception):
    pass


class MutexStateError(Exception):
    pass


LAMPORT_EVENT_ACTIONS = {
    "LOCAL",
    "SEND",
    "RECEIVE",
    "MUTEX_WANTED",
    "ENTER_CS",
    "EXIT_CS",
    "ELECTION_STARTED",
    "BECAME_LEADER",
    "LEADER_TIMEOUT",
    "MUTEX_ABORTED",
    "ELECTION_ROUND_TIMED_OUT",
    "ELECTION_RETRY_SCHEDULED",
    "ELECTION_ROUND_RESET",
}

ANNOTATION_ACTIONS = {
    "SEND_FAILED",
    "MUTEX_REQUEST_RECEIVED",
    "MUTEX_REPLY_RECEIVED",
    "MUTEX_REPLY_DEFERRED",
    "MUTEX_HELD",
    "MUTEX_RELEASED",
    "ELECTION_RECEIVED",
    "ELECTION_OK_RECEIVED",
    "ELECTION_OK_IGNORED",
    "COORDINATOR_RECEIVED",
    "COORDINATOR_REJECTED",
    "LEADER_CHANGED",
    "HEARTBEAT_RECEIVED",
    "HEARTBEAT_REJECTED",
}


def classify_event_kind(action: str) -> str:
    if action in LAMPORT_EVENT_ACTIONS:
        return "lamport_event"
    if action in ANNOTATION_ACTIONS:
        return "annotation"
    return "annotation"

class NodeState:
    def __init__(
        self,
        node_id: int,
        peers: dict[int, str],
        max_events: int = 500,
    ) -> None:
        self.node_id = node_id
        self.peers = dict(peers)
        self.clock = LamportClock()
        self.messages_sent = 0
        self.messages_received = 0
        self.mutex_state = RELEASED
        self.current_request_id: str | None = None
        self.request_timestamp: int | None = None
        self.replies_received: set[int] = set()
        self.deferred_replies: dict[int, str] = {}
        self.wait_started_at: float | None = None
        self._reply_event: asyncio.Event | None = None
        self.leader_id: int | None = None
        self.election_in_progress = False
        self.election_id: str | None = None
        self.election_started_at: float | None = None
        self.election_ok_received: set[int] = set()
        self.last_leader_heartbeat: float | None = None
        self.last_leader_change_wall_time: str | None = None
        self.elections_started = 0
        self._events: deque[EventRecord] = deque(maxlen=max_events)
        self._lock = asyncio.Lock()

    async def get_peer_url(self, destination_id: int) -> str | None:
        async with self._lock:
            return self.peers.get(destination_id)

    async def get_peer_items(self) -> list[tuple[int, str]]:
        async with self._lock:
            return sorted(self.peers.items())


    async def is_current_leader(self) -> bool:
        async with self._lock:
            return self.leader_id == self.node_id

    async def record_local_event(self, description: str) -> EventRecord:
        async with self._lock:
            logical_time = self.clock.tick()
            event = self._new_event(
                logical_time=logical_time,
                action="LOCAL",
                detail=description,
            )
            self._events.append(event)
        self._print_event(event)
        return event

    async def prepare_app_message(
        self,
        destination_id: int,
        message_id: str,
        text: str,
    ) -> tuple[AppMessage, EventRecord]:
        return await self.prepare_message(
            destination_id=destination_id,
            message_type=APP_MESSAGE,
            message_id=message_id,
            payload={"text": text},
            detail=text,
        )

    async def prepare_message(
        self,
        destination_id: int,
        message_type: str,
        message_id: str,
        payload: dict[str, object],
        detail: str = "",
    ) -> tuple[AppMessage, EventRecord]:
        async with self._lock:
            logical_time = self.clock.tick()
            message = AppMessage(
                type=message_type,
                sender_id=self.node_id,
                message_id=message_id,
                logical_time=logical_time,
                payload=dict(payload),
            )
            event = self._new_event(
                logical_time=logical_time,
                action="SEND",
                peer_id=destination_id,
                peer_role="destination",
                message=message,
                detail=detail,
            )
            self._events.append(event)
        self._print_event(event)
        return message, event

    async def record_message_event(
        self,
        action: str,
        peer_id: int,
        peer_role: str,
        message: AppMessage,
        detail: str = "",
    ) -> EventRecord:
        async with self._lock:
            event = self._new_event(
                logical_time=message.logical_time,
                action=action,
                peer_id=peer_id,
                peer_role=peer_role,
                message=message,
                detail=detail,
            )
            self._events.append(event)
        self._print_event(event)
        return event
    async def confirm_sent(self) -> None:
        async with self._lock:
            self.messages_sent += 1

    async def record_send_failed(
        self,
        destination_id: int,
        message: AppMessage,
        error: str,
    ) -> EventRecord:
        async with self._lock:
            event = self._new_event(
                logical_time=message.logical_time,
                action="SEND_FAILED",
                peer_id=destination_id,
                peer_role="destination",
                message=message,
                detail=error,
            )
            self._events.append(event)
        self._print_event(event)
        return event

    async def record_received(self, message: AppMessage) -> EventRecord:
        async with self._lock:
            logical_time = self.clock.update(message.logical_time)
            self.messages_received += 1
            event = self._new_event(
                logical_time=logical_time,
                action="RECEIVE",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                detail=str(message.payload.get("text", "")),
            )
            self._events.append(event)
        self._print_event(event)
        return event

    async def begin_mutex_request(self, request_id: str) -> tuple[int, asyncio.Event, list[tuple[int, str]]]:
        async with self._lock:
            if self.mutex_state != RELEASED:
                raise MutexBusyError(f"mutex is {self.mutex_state}")
            request_timestamp = self.clock.tick()
            self.mutex_state = WANTED
            self.current_request_id = request_id
            self.request_timestamp = request_timestamp
            self.replies_received = set()
            self.wait_started_at = time.monotonic()
            self._reply_event = asyncio.Event()
            reply_event = self._reply_event
            event = self._new_event(
                logical_time=request_timestamp,
                action="MUTEX_WANTED",
                request_id=request_id,
                request_timestamp=request_timestamp,
                detail="waiting for replies",
            )
            self._events.append(event)
            peers = sorted(self.peers.items())
            if not peers:
                reply_event.set()
        self._print_event(event)
        return request_timestamp, reply_event, peers

    async def handle_mutex_request(
        self,
        message: AppMessage,
        request_id: str,
        remote_request_timestamp: int,
    ) -> bool:
        async with self._lock:
            logical_time = self.clock.update(message.logical_time)
            self.messages_received += 1
            receive_event = self._new_event(
                logical_time=logical_time,
                action="RECEIVE",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                request_id=request_id,
                request_timestamp=remote_request_timestamp,
                detail="mutex request",
            )
            request_event = self._new_event(
                logical_time=logical_time,
                action="MUTEX_REQUEST_RECEIVED",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                request_id=request_id,
                request_timestamp=remote_request_timestamp,
                detail=f"state={self.mutex_state}",
            )
            self._events.append(receive_event)
            self._events.append(request_event)
            defer = should_defer_reply(
                self.mutex_state,
                self.request_timestamp,
                self.node_id,
                remote_request_timestamp,
                message.sender_id,
            )
            deferred_event = None
            if defer:
                self.deferred_replies[message.sender_id] = request_id
                deferred_event = self._new_event(
                    logical_time=logical_time,
                    action="MUTEX_REPLY_DEFERRED",
                    peer_id=message.sender_id,
                    peer_role="source",
                    message=message,
                    request_id=request_id,
                    request_timestamp=remote_request_timestamp,
                    detail="reply deferred",
                )
                self._events.append(deferred_event)
        self._print_event(receive_event)
        self._print_event(request_event)
        if deferred_event:
            self._print_event(deferred_event)
        return not defer

    async def handle_mutex_reply(self, message: AppMessage, request_id: str) -> bool:
        async with self._lock:
            logical_time = self.clock.update(message.logical_time)
            self.messages_received += 1
            receive_event = self._new_event(
                logical_time=logical_time,
                action="RECEIVE",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                request_id=request_id,
                detail="mutex reply",
            )
            self._events.append(receive_event)
            accepted = False
            reply_event = None
            if self.mutex_state == WANTED and request_id == self.current_request_id:
                before = len(self.replies_received)
                self.replies_received.add(message.sender_id)
                accepted = len(self.replies_received) > before
                if accepted:
                    reply_event = self._new_event(
                        logical_time=logical_time,
                        action="MUTEX_REPLY_RECEIVED",
                        peer_id=message.sender_id,
                        peer_role="source",
                        message=message,
                        request_id=request_id,
                        request_timestamp=self.request_timestamp,
                        detail=f"replies={len(self.replies_received)}/{len(self.peers)}",
                    )
                    self._events.append(reply_event)
                if len(self.replies_received) == len(self.peers) and self._reply_event:
                    self._reply_event.set()
        self._print_event(receive_event)
        if reply_event:
            self._print_event(reply_event)
        return accepted

    async def mark_mutex_held(self, request_id: str) -> tuple[int, float]:
        async with self._lock:
            if self.mutex_state != WANTED or self.current_request_id != request_id:
                raise MutexStateError("mutex request is not waiting")
            if len(self.replies_received) != len(self.peers):
                raise MutexStateError("not all replies were received")
            self.mutex_state = HELD
            wait_ms = 0.0
            if self.wait_started_at is not None:
                wait_ms = (time.monotonic() - self.wait_started_at) * 1000
            logical_time = self.clock.tick()
            event = self._new_event(
                logical_time=logical_time,
                action="MUTEX_HELD",
                request_id=request_id,
                request_timestamp=self.request_timestamp,
                detail="all replies received",
            )
            self._events.append(event)
        self._print_event(event)
        return logical_time, wait_ms

    async def enter_critical_section(self, request_id: str) -> EventRecord:
        async with self._lock:
            if self.mutex_state != HELD or self.current_request_id != request_id:
                raise MutexStateError("mutex is not held for this request")
            logical_time = self.clock.tick()
            event = self._new_event(
                logical_time=logical_time,
                action="ENTER_CS",
                request_id=request_id,
                request_timestamp=self.request_timestamp,
                detail="enter critical section",
            )
            self._events.append(event)
        self._print_event(event)
        return event

    async def exit_critical_section(self, request_id: str) -> tuple[EventRecord, list[tuple[int, str]]]:
        async with self._lock:
            if self.mutex_state != HELD or self.current_request_id != request_id:
                raise MutexStateError("mutex is not held for this request")
            logical_time = self.clock.tick()
            exit_event = self._new_event(
                logical_time=logical_time,
                action="EXIT_CS",
                request_id=request_id,
                request_timestamp=self.request_timestamp,
                detail="exit critical section",
            )
            deferred = sorted(self.deferred_replies.items())
            self._clear_mutex_request()
            released_event = self._new_event(
                logical_time=logical_time,
                action="MUTEX_RELEASED",
                request_id=request_id,
                detail="released",
            )
            self._events.append(exit_event)
            self._events.append(released_event)
        self._print_event(exit_event)
        self._print_event(released_event)
        return exit_event, deferred

    async def abort_mutex_request(self, request_id: str, reason: str) -> list[tuple[int, str]]:
        async with self._lock:
            if self.current_request_id != request_id:
                return []
            logical_time = self.clock.tick()
            deferred = sorted(self.deferred_replies.items())
            self._clear_mutex_request()
            event = self._new_event(
                logical_time=logical_time,
                action="MUTEX_ABORTED",
                request_id=request_id,
                detail=reason,
            )
            self._events.append(event)
        self._print_event(event)
        return deferred

    async def begin_election(self, election_id: str, now: float, reason: str) -> tuple[bool, list[tuple[int, str]]]:
        async with self._lock:
            if self.election_in_progress:
                return False, []
            logical_time = self.clock.tick()
            self.election_in_progress = True
            self.election_id = election_id
            self.election_started_at = now
            self.election_ok_received = set()
            self.elections_started += 1
            event = self._new_event(
                logical_time=logical_time,
                action="ELECTION_STARTED",
                election_id=election_id,
                detail=reason,
            )
            self._events.append(event)
            peers = higher_peers(self.node_id, self.peers)
        self._print_event(event)
        return True, peers

    async def election_has_ok(self, election_id: str) -> bool:
        async with self._lock:
            return self.election_in_progress and self.election_id == election_id and bool(self.election_ok_received)

    async def should_restart_election(self, election_id: str) -> bool:
        async with self._lock:
            return self.election_in_progress and self.election_id == election_id

    async def become_leader(self, election_id: str, now: float) -> tuple[bool, list[tuple[int, str]], EventRecord | None]:
        async with self._lock:
            if not self.election_in_progress or self.election_id != election_id:
                return False, [], None
            logical_time = self.clock.tick()
            self.leader_id = self.node_id
            self.election_in_progress = False
            self.election_id = None
            self.election_started_at = None
            self.election_ok_received = set()
            self.last_leader_heartbeat = now
            self.last_leader_change_wall_time = datetime.now().isoformat(timespec="milliseconds")
            event = self._new_event(
                logical_time=logical_time,
                action="BECAME_LEADER",
                leader_id=self.node_id,
                detail="highest active id observed",
            )
            self._events.append(event)
            peers = sorted(self.peers.items())
        self._print_event(event)
        return True, peers, event

    async def handle_election(self, message: AppMessage, election_id: str) -> bool:
        async with self._lock:
            logical_time = self.clock.update(message.logical_time)
            self.messages_received += 1
            receive_event = self._new_event(
                logical_time=logical_time,
                action="RECEIVE",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                election_id=election_id,
                detail="election request",
            )
            election_event = self._new_event(
                logical_time=logical_time,
                action="ELECTION_RECEIVED",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                election_id=election_id,
                detail="will answer" if message.sender_id < self.node_id else "ignored sender with same/higher id",
            )
            self._events.append(receive_event)
            self._events.append(election_event)
            should_reply = message.sender_id < self.node_id
        self._print_event(receive_event)
        self._print_event(election_event)
        return should_reply

    async def handle_election_ok(self, message: AppMessage, election_id: str) -> bool:
        async with self._lock:
            logical_time = self.clock.update(message.logical_time)
            self.messages_received += 1
            receive_event = self._new_event(
                logical_time=logical_time,
                action="RECEIVE",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                election_id=election_id,
                detail="election ok",
            )
            accepted = should_accept_election_ok(
                self.node_id,
                message.sender_id,
                self.election_id,
                election_id,
                self.election_in_progress,
            )
            if accepted:
                before = len(self.election_ok_received)
                self.election_ok_received.add(message.sender_id)
                accepted = len(self.election_ok_received) > before
            action = "ELECTION_OK_RECEIVED" if accepted else "ELECTION_OK_IGNORED"
            ok_event = self._new_event(
                logical_time=logical_time,
                action=action,
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                election_id=election_id,
                detail="accepted" if accepted else "stale, duplicate, or invalid",
            )
            self._events.append(receive_event)
            self._events.append(ok_event)
        self._print_event(receive_event)
        self._print_event(ok_event)
        return accepted

    async def handle_coordinator(self, message: AppMessage, leader_id: int) -> bool:
        async with self._lock:
            logical_time = self.clock.update(message.logical_time)
            self.messages_received += 1
            receive_event = self._new_event(
                logical_time=logical_time,
                action="RECEIVE",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                leader_id=leader_id,
                detail="coordinator",
            )
            accepted = should_accept_coordinator(self.node_id, self.leader_id, leader_id)
            action = "LEADER_CHANGED" if accepted and self.leader_id != leader_id else ("COORDINATOR_RECEIVED" if accepted else "COORDINATOR_REJECTED")
            if accepted:
                self.leader_id = leader_id
                self.election_in_progress = False
                self.election_id = None
                self.election_started_at = None
                self.election_ok_received = set()
                self.last_leader_heartbeat = time.monotonic()
                self.last_leader_change_wall_time = datetime.now().isoformat(timespec="milliseconds")
                detail = "accepted"
            else:
                detail = "ignored lower-priority coordinator"
            coord_event = self._new_event(
                logical_time=logical_time,
                action=action,
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                leader_id=leader_id,
                detail=detail,
            )
            self._events.append(receive_event)
            self._events.append(coord_event)
        self._print_event(receive_event)
        self._print_event(coord_event)
        return accepted

    async def handle_heartbeat(self, message: AppMessage, leader_id: int) -> bool:
        async with self._lock:
            logical_time = self.clock.update(message.logical_time)
            self.messages_received += 1
            receive_event = self._new_event(
                logical_time=logical_time,
                action="RECEIVE",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                leader_id=leader_id,
                detail="heartbeat",
            )
            valid = should_accept_heartbeat(self.node_id, self.leader_id, message.sender_id, leader_id)
            if valid:
                if self.leader_id != leader_id:
                    self.last_leader_change_wall_time = datetime.now().isoformat(timespec="milliseconds")
                self.leader_id = leader_id
                self.last_leader_heartbeat = time.monotonic()
            heartbeat_event = self._new_event(
                logical_time=logical_time,
                action="HEARTBEAT_RECEIVED" if valid else "HEARTBEAT_REJECTED",
                peer_id=message.sender_id,
                peer_role="source",
                message=message,
                leader_id=leader_id,
                detail="accepted" if valid else "ignored invalid leader",
            )
            self._events.append(receive_event)
            self._events.append(heartbeat_event)
        self._print_event(receive_event)
        self._print_event(heartbeat_event)
        return valid

    async def record_leader_timeout(self, now: float, timeout_seconds: float) -> bool:
        async with self._lock:
            if self.leader_id is None or self.leader_id == self.node_id or self.election_in_progress:
                return False
            if self.last_leader_heartbeat is None:
                elapsed = timeout_seconds + 1
            else:
                elapsed = now - self.last_leader_heartbeat
            if elapsed <= timeout_seconds:
                return False
            old_leader = self.leader_id
            self.leader_id = None
            self.last_leader_heartbeat = None
            logical_time = self.clock.tick()
            event = self._new_event(
                logical_time=logical_time,
                action="LEADER_TIMEOUT",
                leader_id=old_leader,
                detail=f"elapsed_ms={elapsed * 1000:.0f}",
            )
            self._events.append(event)
        self._print_event(event)
        return True

    async def reset_election_for_retry(self, election_id: str, reason: str) -> bool:
        async with self._lock:
            if not self.election_in_progress or self.election_id != election_id:
                return False
            logical_time = self.clock.tick()
            old_ok = sorted(self.election_ok_received)
            self.election_in_progress = False
            self.election_id = None
            self.election_started_at = None
            self.election_ok_received = set()
            event = self._new_event(
                logical_time=logical_time,
                action="ELECTION_ROUND_TIMED_OUT",
                election_id=election_id,
                detail=f"retry after {reason}; previous_ok={old_ok}",
            )
            self._events.append(event)
        self._print_event(event)
        return True
    async def snapshot(self) -> dict[str, object]:
        async with self._lock:
            return {
                "node_id": self.node_id,
                "peers": dict(self.peers),
                "logical_clock": self.clock.value,
                "messages_sent": self.messages_sent,
                "messages_received": self.messages_received,
                "events_stored": len(self._events),
                "mutex": {
                    "state": self.mutex_state,
                    "request_id": self.current_request_id,
                    "request_timestamp": self.request_timestamp,
                    "replies_received": sorted(self.replies_received),
                    "deferred_replies": [
                        {"sender_id": sender_id, "request_id": request_id}
                        for sender_id, request_id in sorted(self.deferred_replies.items())
                    ],
                },
                "election": {
                    "leader_id": self.leader_id,
                    "is_leader": self.leader_id == self.node_id,
                    "election_in_progress": self.election_in_progress,
                    "election_id": self.election_id,
                    "ok_received_from": sorted(self.election_ok_received),
                    "elections_started": self.elections_started,
                    "last_leader_change_wall_time": self.last_leader_change_wall_time,
                },
            }

    async def events(self) -> list[dict[str, object]]:
        async with self._lock:
            events = list(self._events)
        return [event_as_dict(event) for event in events]

    def _clear_mutex_request(self) -> None:
        self.mutex_state = RELEASED
        self.current_request_id = None
        self.request_timestamp = None
        self.replies_received = set()
        self.deferred_replies = {}
        self.wait_started_at = None
        self._reply_event = None

    def _new_event(
        self,
        logical_time: int,
        action: str,
        peer_id: int | None = None,
        peer_role: str | None = None,
        message: AppMessage | None = None,
        request_id: str | None = None,
        request_timestamp: int | None = None,
        election_id: str | None = None,
        leader_id: int | None = None,
        detail: str = "",
    ) -> EventRecord:
        if request_id is None and message is not None:
            raw_request_id = message.payload.get("request_id")
            request_id = raw_request_id if isinstance(raw_request_id, str) else None
        if request_timestamp is None and message is not None:
            raw_request_timestamp = message.payload.get("request_timestamp")
            request_timestamp = raw_request_timestamp if isinstance(raw_request_timestamp, int) else None
        if election_id is None and message is not None:
            raw_election_id = message.payload.get("election_id")
            election_id = raw_election_id if isinstance(raw_election_id, str) else None
        if leader_id is None and message is not None:
            raw_leader_id = message.payload.get("leader_id")
            leader_id = raw_leader_id if isinstance(raw_leader_id, int) else None
        return EventRecord(
            wall_time=datetime.now().isoformat(timespec="milliseconds"),
            logical_time=logical_time,
            node_id=self.node_id,
            action=action,
            peer_id=peer_id,
            peer_role=peer_role,
            message_type=message.type if message else None,
            message_id=message.message_id if message else None,
            request_id=request_id,
            request_timestamp=request_timestamp,
            election_id=election_id,
            leader_id=leader_id,
            event_kind=classify_event_kind(action),
            detail=detail,
        )

    def _print_event(self, event: EventRecord) -> None:
        parts = [
            f"wall={event.wall_time}",
            f"L={event.logical_time}",
            f"node={event.node_id}",
            f"{event.action:<22}",
        ]
        if event.peer_role and event.peer_id is not None:
            parts.append(f"{event.peer_role}={event.peer_id}")
        if event.message_type:
            parts.append(f"type={event.message_type}")
        if event.message_id:
            parts.append(f"id={event.message_id}")
        if event.request_id:
            parts.append(f"request={event.request_id}")
        if event.request_timestamp is not None:
            parts.append(f"request_ts={event.request_timestamp}")
        if event.election_id:
            parts.append(f"election={event.election_id}")
        if event.leader_id is not None:
            parts.append(f"leader={event.leader_id}")
        if event.detail:
            parts.append(f"detail={event.detail}")
        print(" | ".join(parts), flush=True)


def event_as_dict(event: EventRecord) -> dict[str, object]:
    if hasattr(event, "model_dump"):
        return event.model_dump()
    return event.dict()
