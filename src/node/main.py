import asyncio
import os
import time
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException

from .election import ElectionConfig
from .models import (
    APP_MESSAGE,
    COORDINATOR,
    ELECTION,
    ELECTION_OK,
    HEARTBEAT,
    MUTEX_REPLY,
    MUTEX_REQUEST,
    AppMessage,
    CriticalSectionCommand,
    LocalEventCommand,
    SendMessageCommand,
    StartElectionCommand,
)
from .state import MutexBusyError, MutexStateError, NodeState
from .transport import post_json, send_message


def parse_peers(raw_peers: str) -> dict[int, str]:
    peers: dict[int, str] = {}
    if not raw_peers.strip():
        return peers

    for item in raw_peers.split(","):
        entry = item.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError(f"invalid peer entry {entry!r}; expected ID=URL")
        peer_id, url = entry.split("=", 1)
        peer_id = peer_id.strip()
        url = url.strip()
        if not peer_id.isdigit() or not url:
            raise ValueError(f"invalid peer entry {entry!r}; expected numeric ID and URL")
        peers[int(peer_id)] = url
    return peers


def parse_positive_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def load_election_config() -> ElectionConfig:
    config = ElectionConfig(
        heartbeat_interval_ms=parse_positive_int_env("HEARTBEAT_INTERVAL_MS", 700),
        leader_timeout_ms=parse_positive_int_env("LEADER_TIMEOUT_MS", 2500),
        election_response_timeout_ms=parse_positive_int_env("ELECTION_RESPONSE_TIMEOUT_MS", 700),
        coordinator_timeout_ms=parse_positive_int_env("COORDINATOR_TIMEOUT_MS", 2500),
        startup_election_delay_ms=parse_positive_int_env("STARTUP_ELECTION_DELAY_MS", 500),
    )
    config.validate()
    return config


NODE_ID = int(os.getenv("NODE_ID", "1"))
PEERS = parse_peers(os.getenv("PEERS", ""))
RESOURCE_URL = os.getenv("RESOURCE_URL", "http://resource:8000")
MUTEX_TIMEOUT_SECONDS = float(os.getenv("MUTEX_TIMEOUT_SECONDS", "10"))
ELECTION_CONFIG = load_election_config()

state = NodeState(node_id=NODE_ID, peers=PEERS)
_election_task: asyncio.Task[None] | None = None
_monitor_task: asyncio.Task[None] | None = None
_task_lock = asyncio.Lock()
_shutdown = asyncio.Event()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _monitor_task
    _shutdown.clear()
    _monitor_task = asyncio.create_task(heartbeat_and_failure_monitor())
    try:
        yield
    finally:
        _shutdown.set()
        tasks = [task for task in (_monitor_task, _election_task) if task is not None and not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(title=f"MC714 node {NODE_ID}", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, object]:
    return {"status": "ok", "node_id": NODE_ID}


@app.get("/state")
async def get_state() -> dict[str, object]:
    return await state.snapshot()


@app.get("/events")
async def get_events() -> list[dict[str, object]]:
    return await state.events()


@app.post("/messages")
async def receive_message(message: AppMessage) -> dict[str, object]:
    if message.type == APP_MESSAGE:
        event = await state.record_received(message)
        return {
            "status": "received",
            "node_id": NODE_ID,
            "message_id": message.message_id,
            "logical_time": event.logical_time,
        }
    if message.type == MUTEX_REQUEST:
        return await receive_mutex_request(message)
    if message.type == MUTEX_REPLY:
        return await receive_mutex_reply(message)
    if message.type == ELECTION:
        return await receive_election(message)
    if message.type == ELECTION_OK:
        return await receive_election_ok(message)
    if message.type == COORDINATOR:
        return await receive_coordinator(message)
    if message.type == HEARTBEAT:
        return await receive_heartbeat(message)
    raise HTTPException(status_code=400, detail=f"unknown message type {message.type!r}")


@app.post("/commands/local-event")
async def command_local_event(command: LocalEventCommand) -> dict[str, object]:
    event = await state.record_local_event(command.description)
    return {
        "status": "recorded",
        "node_id": NODE_ID,
        "logical_time": event.logical_time,
    }


@app.post("/commands/send-message")
async def command_send_message(command: SendMessageCommand) -> dict[str, object]:
    peer_url = await state.get_peer_url(command.destination_id)
    if peer_url is None:
        raise HTTPException(
            status_code=404,
            detail=f"destination node {command.destination_id} is not configured as a peer",
        )

    message, event = await state.prepare_app_message(
        destination_id=command.destination_id,
        message_id=str(uuid4()),
        text=command.text,
    )

    try:
        result = await send_message(peer_url, message)
    except httpx.HTTPError as exc:
        await state.record_send_failed(command.destination_id, message, str(exc))
        raise HTTPException(status_code=502, detail=f"failed to send message: {exc}") from exc

    await state.confirm_sent()
    return {
        "status": "sent",
        "node_id": NODE_ID,
        "destination_id": command.destination_id,
        "message_id": message.message_id,
        "logical_time": event.logical_time,
        "destination_response": result,
    }


@app.post("/commands/request-critical-section")
async def command_request_critical_section(command: CriticalSectionCommand) -> dict[str, object]:
    request_id = str(uuid4())
    try:
        request_timestamp, reply_event, peers = await state.begin_mutex_request(request_id)
    except MutexBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    try:
        for peer_id, peer_url in peers:
            await send_mutex_request(peer_id, peer_url, request_id, request_timestamp)

        try:
            await asyncio.wait_for(reply_event.wait(), timeout=MUTEX_TIMEOUT_SECONDS)
        except asyncio.TimeoutError as exc:
            deferred = await state.abort_mutex_request(request_id, "timeout waiting for mutex replies")
            await send_deferred_replies(deferred)
            raise HTTPException(status_code=504, detail="timeout waiting for mutex replies") from exc

        _, wait_ms = await state.mark_mutex_held(request_id)
        enter_event = await state.enter_critical_section(request_id)
        await notify_resource("enter", request_id, enter_event.logical_time)
        await asyncio.sleep(command.duration_ms / 1000)
        exit_event, deferred = await state.exit_critical_section(request_id)
        await notify_resource("exit", request_id, exit_event.logical_time)
        await send_deferred_replies(deferred)
    except HTTPException:
        raise
    except (httpx.HTTPError, MutexStateError) as exc:
        deferred = await state.abort_mutex_request(request_id, f"mutex failed: {exc}")
        await send_deferred_replies(deferred)
        raise HTTPException(status_code=502, detail=f"mutex request failed: {exc}") from exc

    return {
        "request_id": request_id,
        "request_timestamp": request_timestamp,
        "status": "completed",
        "wait_ms": round(wait_ms, 3),
    }


@app.post("/commands/start-election", status_code=202)
async def command_start_election(command: StartElectionCommand) -> dict[str, object]:
    started = await schedule_election(f"manual: {command.reason}")
    return {"status": "started" if started else "already_in_progress", "node_id": NODE_ID}


async def receive_mutex_request(message: AppMessage) -> dict[str, object]:
    request_id = require_string_payload(message, "request_id")
    request_timestamp = require_int_payload(message, "request_timestamp")
    should_reply = await state.handle_mutex_request(message, request_id, request_timestamp)
    if should_reply:
        peer_url = await state.get_peer_url(message.sender_id)
        if peer_url is None:
            raise HTTPException(status_code=404, detail="sender is not configured as a peer")
        await send_mutex_reply(message.sender_id, peer_url, request_id)
    return {"status": "processed", "node_id": NODE_ID, "reply_sent": should_reply}


async def receive_mutex_reply(message: AppMessage) -> dict[str, object]:
    request_id = require_string_payload(message, "request_id")
    accepted = await state.handle_mutex_reply(message, request_id)
    return {"status": "processed", "node_id": NODE_ID, "accepted": accepted}


async def receive_election(message: AppMessage) -> dict[str, object]:
    election_id = require_string_payload(message, "election_id")
    should_reply = await state.handle_election(message, election_id)
    if should_reply:
        peer_url = await state.get_peer_url(message.sender_id)
        if peer_url is None:
            raise HTTPException(status_code=404, detail="sender is not configured as a peer")
        await send_election_ok(message.sender_id, peer_url, election_id)
        await schedule_election(f"received election from node {message.sender_id}")
    return {"status": "processed", "node_id": NODE_ID, "ok_sent": should_reply}


async def receive_election_ok(message: AppMessage) -> dict[str, object]:
    election_id = require_string_payload(message, "election_id")
    accepted = await state.handle_election_ok(message, election_id)
    return {"status": "processed", "node_id": NODE_ID, "accepted": accepted}


async def receive_coordinator(message: AppMessage) -> dict[str, object]:
    leader_id = require_int_payload(message, "leader_id")
    if leader_id != message.sender_id:
        await state.record_received(message)
        raise HTTPException(status_code=400, detail="payload.leader_id must match sender_id")
    accepted = await state.handle_coordinator(message, leader_id)
    return {"status": "processed", "node_id": NODE_ID, "accepted": accepted, "leader_id": leader_id}


async def receive_heartbeat(message: AppMessage) -> dict[str, object]:
    leader_id = require_int_payload(message, "leader_id")
    accepted = await state.handle_heartbeat(message, leader_id)
    return {"status": "processed", "node_id": NODE_ID, "accepted": accepted, "leader_id": leader_id}


async def schedule_election(reason: str) -> bool:
    global _election_task
    async with _task_lock:
        if _election_task is not None and not _election_task.done():
            return False
        _election_task = asyncio.create_task(run_election(reason))
        return True


async def run_election(reason: str) -> None:
    next_reason = reason
    while not _shutdown.is_set():
        election_id = str(uuid4())
        started, higher = await state.begin_election(election_id, time.monotonic(), next_reason)
        if not started:
            return

        for peer_id, peer_url in higher:
            await send_election(peer_id, peer_url, election_id)

        if not higher:
            became, peers, _ = await state.become_leader(election_id, time.monotonic())
            if became:
                for peer_id, peer_url in peers:
                    await send_coordinator(peer_id, peer_url)
            return

        await asyncio.sleep(ELECTION_CONFIG.election_response_timeout_seconds)
        if not await state.election_has_ok(election_id):
            became, peers, _ = await state.become_leader(election_id, time.monotonic())
            if became:
                for peer_id, peer_url in peers:
                    await send_coordinator(peer_id, peer_url)
            return

        await asyncio.sleep(ELECTION_CONFIG.coordinator_timeout_seconds)
        if not await state.should_restart_election(election_id):
            return
        await state.reset_election_for_retry(election_id, "coordinator timeout")
        next_reason = "coordinator timeout"


async def heartbeat_and_failure_monitor() -> None:
    await asyncio.sleep(ELECTION_CONFIG.startup_election_delay_seconds)
    await schedule_election("startup")
    last_heartbeat_sent = 0.0
    poll_interval = min(0.25, ELECTION_CONFIG.heartbeat_interval_seconds)
    while not _shutdown.is_set():
        now = time.monotonic()
        if await state.is_current_leader():
            if now - last_heartbeat_sent >= ELECTION_CONFIG.heartbeat_interval_seconds:
                peers = await state.get_peer_items()
                for peer_id, peer_url in peers:
                    await send_heartbeat(peer_id, peer_url)
                last_heartbeat_sent = now
        else:
            timed_out = await state.record_leader_timeout(now, ELECTION_CONFIG.leader_timeout_seconds)
            if timed_out:
                await schedule_election("leader timeout")
        await asyncio.sleep(poll_interval)


async def send_mutex_request(
    peer_id: int,
    peer_url: str,
    request_id: str,
    request_timestamp: int,
) -> None:
    payload = {"request_id": request_id, "request_timestamp": request_timestamp}
    message, _ = await state.prepare_message(
        destination_id=peer_id,
        message_type=MUTEX_REQUEST,
        message_id=str(uuid4()),
        payload=payload,
        detail="mutex request",
    )
    try:
        await send_message(peer_url, message)
    except httpx.HTTPError as exc:
        await state.record_send_failed(peer_id, message, str(exc))
        raise
    await state.confirm_sent()


async def send_mutex_reply(peer_id: int, peer_url: str, request_id: str) -> None:
    message, _ = await state.prepare_message(
        destination_id=peer_id,
        message_type=MUTEX_REPLY,
        message_id=str(uuid4()),
        payload={"request_id": request_id},
        detail="mutex reply",
    )
    try:
        await send_message(peer_url, message)
    except httpx.HTTPError as exc:
        await state.record_send_failed(peer_id, message, str(exc))
        raise
    await state.confirm_sent()


async def send_election(peer_id: int, peer_url: str, election_id: str) -> None:
    await send_election_protocol_message(
        peer_id,
        peer_url,
        ELECTION,
        {"election_id": election_id},
        "ELECTION_SENT",
        raise_on_failure=False,
    )


async def send_election_ok(peer_id: int, peer_url: str, election_id: str) -> None:
    await send_election_protocol_message(
        peer_id,
        peer_url,
        ELECTION_OK,
        {"election_id": election_id},
        "ELECTION_OK_SENT",
        raise_on_failure=False,
    )


async def send_coordinator(peer_id: int, peer_url: str) -> None:
    await send_election_protocol_message(
        peer_id,
        peer_url,
        COORDINATOR,
        {"leader_id": NODE_ID},
        "COORDINATOR_SENT",
        raise_on_failure=False,
    )


async def send_heartbeat(peer_id: int, peer_url: str) -> None:
    await send_election_protocol_message(
        peer_id,
        peer_url,
        HEARTBEAT,
        {"leader_id": NODE_ID},
        "HEARTBEAT_SENT",
        raise_on_failure=False,
    )


async def send_election_protocol_message(
    peer_id: int,
    peer_url: str,
    message_type: str,
    payload: dict[str, object],
    action: str,
    raise_on_failure: bool = True,
) -> None:
    message, _ = await state.prepare_message(
        destination_id=peer_id,
        message_type=message_type,
        message_id=str(uuid4()),
        payload=payload,
        detail=message_type.lower(),
    )
    await state.record_message_event(action, peer_id, "destination", message, message_type.lower())
    try:
        await send_message(peer_url, message)
    except httpx.HTTPError as exc:
        await state.record_send_failed(peer_id, message, str(exc))
        if raise_on_failure:
            raise
        return
    await state.confirm_sent()


async def send_deferred_replies(deferred: list[tuple[int, str]]) -> None:
    for peer_id, request_id in deferred:
        peer_url = await state.get_peer_url(peer_id)
        if peer_url is not None:
            await send_mutex_reply(peer_id, peer_url, request_id)


async def notify_resource(action: str, request_id: str, logical_time: int) -> None:
    await post_json(
        f"{RESOURCE_URL.rstrip('/')}/{action}",
        {"node_id": NODE_ID, "request_id": request_id, "logical_time": logical_time},
    )


def require_string_payload(message: AppMessage, field: str) -> str:
    value = message.payload.get(field)
    if not isinstance(value, str) or not value:
        raise HTTPException(status_code=400, detail=f"payload.{field} must be a non-empty string")
    return value


def require_int_payload(message: AppMessage, field: str) -> int:
    value = message.payload.get(field)
    if not isinstance(value, int) or value < 0:
        raise HTTPException(status_code=400, detail=f"payload.{field} must be a non-negative integer")
    return value

