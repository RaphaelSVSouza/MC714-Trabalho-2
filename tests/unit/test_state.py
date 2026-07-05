import asyncio

import pytest

from node.models import APP_MESSAGE, MUTEX_REPLY, AppMessage
from node.mutex import HELD, RELEASED, WANTED
from node.state import MutexBusyError, MutexStateError, NodeState


@pytest.mark.asyncio
async def test_local_event_increments_clock_and_stores_event() -> None:
    state = NodeState(node_id=1, peers={})

    event = await state.record_local_event("internal work")
    snapshot = await state.snapshot()

    assert event.action == "LOCAL"
    assert event.logical_time == 1
    assert snapshot["logical_clock"] == 1


@pytest.mark.asyncio
async def test_prepare_app_message_ticks_once_and_captures_timestamp() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})

    message, event = await state.prepare_app_message(2, "msg-1", "hello")
    snapshot = await state.snapshot()
    events = await state.events()

    assert message.type == APP_MESSAGE
    assert message.logical_time == 1
    assert event.logical_time == 1
    assert events[0]["logical_time"] == 1
    assert events[0]["action"] == "SEND"
    assert snapshot["logical_clock"] == 1
    assert snapshot["messages_sent"] == 0


@pytest.mark.asyncio
async def test_confirm_sent_increments_sent_counter() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})
    await state.prepare_app_message(2, "msg-1", "hello")

    await state.confirm_sent()

    snapshot = await state.snapshot()
    assert snapshot["messages_sent"] == 1


@pytest.mark.asyncio
async def test_record_send_failed_stores_event_without_incrementing_sent_counter() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})
    message, _ = await state.prepare_app_message(2, "msg-fail", "hello")

    await state.record_send_failed(2, message, "connection refused")

    snapshot = await state.snapshot()
    events = await state.events()

    assert snapshot["messages_sent"] == 0
    assert snapshot["messages_received"] == 0
    assert snapshot["logical_clock"] == 1
    assert [event["action"] for event in events] == ["SEND", "SEND_FAILED"]
    assert events[1]["logical_time"] == 1
    assert events[1]["detail"] == "connection refused"


@pytest.mark.asyncio
async def test_record_received_updates_clock_before_storing_event() -> None:
    state = NodeState(node_id=2, peers={1: "http://node1:8000"})
    message = AppMessage(
        type=APP_MESSAGE,
        sender_id=1,
        message_id="msg-2",
        logical_time=5,
        payload={"text": "hello"},
    )

    event = await state.record_received(message)
    snapshot = await state.snapshot()

    assert event.action == "RECEIVE"
    assert event.logical_time == 6
    assert snapshot["logical_clock"] == 6
    assert snapshot["messages_received"] == 1


@pytest.mark.asyncio
async def test_receive_with_smaller_timestamp_still_increments_local_clock() -> None:
    state = NodeState(node_id=2, peers={1: "http://node1:8000"})
    await state.record_local_event("first")
    await state.record_local_event("second")
    message = AppMessage(type=APP_MESSAGE, sender_id=1, message_id="old", logical_time=1, payload={})

    event = await state.record_received(message)

    assert event.logical_time == 3


@pytest.mark.asyncio
async def test_events_are_limited() -> None:
    state = NodeState(node_id=1, peers={}, max_events=2)

    for index in range(3):
        await state.prepare_app_message(2, f"msg-{index}", str(index))

    events = await state.events()

    assert len(events) == 2
    assert events[0]["message_id"] == "msg-1"
    assert events[1]["message_id"] == "msg-2"


@pytest.mark.asyncio
async def test_snapshot_contains_node_configuration_clock_and_mutex() -> None:
    state = NodeState(node_id=3, peers={1: "http://node1:8000", 2: "http://node2:8000"})

    snapshot = await state.snapshot()

    assert snapshot["node_id"] == 3
    assert snapshot["peers"] == {1: "http://node1:8000", 2: "http://node2:8000"}
    assert snapshot["logical_clock"] == 0
    assert snapshot["events_stored"] == 0
    assert snapshot["mutex"]["state"] == RELEASED


@pytest.mark.asyncio
async def test_snapshot_returns_copy_of_peers() -> None:
    state = NodeState(node_id=3, peers={1: "http://node1:8000"})

    snapshot = await state.snapshot()
    snapshot["peers"][1] = "changed"

    next_snapshot = await state.snapshot()
    assert next_snapshot["peers"] == {1: "http://node1:8000"}


@pytest.mark.asyncio
async def test_concurrent_local_events_do_not_reuse_logical_timestamps() -> None:
    state = NodeState(node_id=1, peers={})

    events = await asyncio.gather(
        *(state.record_local_event(f"event-{index}") for index in range(20))
    )
    logical_times = [event.logical_time for event in events]

    assert sorted(logical_times) == list(range(1, 21))
    assert len(set(logical_times)) == 20


@pytest.mark.asyncio
async def test_begin_mutex_request_transitions_released_to_wanted() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})

    request_timestamp, _, peers = await state.begin_mutex_request("req-1")
    snapshot = await state.snapshot()

    assert request_timestamp == 1
    assert peers == [(2, "http://node2:8000"), (3, "http://node3:8000")]
    assert snapshot["mutex"]["state"] == WANTED
    assert snapshot["mutex"]["request_id"] == "req-1"
    assert snapshot["mutex"]["request_timestamp"] == 1


@pytest.mark.asyncio
async def test_second_local_mutex_request_is_rejected() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})
    await state.begin_mutex_request("req-1")

    with pytest.raises(MutexBusyError):
        await state.begin_mutex_request("req-2")


@pytest.mark.asyncio
async def test_wanted_transitions_to_held_only_after_all_replies() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})
    await state.begin_mutex_request("req-1")

    with pytest.raises(MutexStateError):
        await state.mark_mutex_held("req-1")

    await state.handle_mutex_reply(mutex_reply(2, "req-1", 2), "req-1")
    await state.handle_mutex_reply(mutex_reply(3, "req-1", 3), "req-1")
    await state.mark_mutex_held("req-1")

    snapshot = await state.snapshot()
    assert snapshot["mutex"]["state"] == HELD


@pytest.mark.asyncio
async def test_duplicate_reply_is_not_counted_twice() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})
    await state.begin_mutex_request("req-1")

    assert await state.handle_mutex_reply(mutex_reply(2, "req-1", 2), "req-1") is True
    assert await state.handle_mutex_reply(mutex_reply(2, "req-1", 3), "req-1") is False

    snapshot = await state.snapshot()
    assert snapshot["mutex"]["replies_received"] == [2]


@pytest.mark.asyncio
async def test_old_reply_does_not_unlock_current_request() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})
    await state.begin_mutex_request("new-req")

    assert await state.handle_mutex_reply(mutex_reply(2, "old-req", 2), "old-req") is False

    snapshot = await state.snapshot()
    assert snapshot["mutex"]["replies_received"] == []


@pytest.mark.asyncio
async def test_held_to_released_returns_deferred_replies() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})
    await state.begin_mutex_request("req-1")
    await state.handle_mutex_reply(mutex_reply(2, "req-1", 2), "req-1")
    await state.mark_mutex_held("req-1")
    await state.handle_mutex_request(mutex_request(2, "req-2", 5, 3), "req-2", 5)

    _, deferred = await state.exit_critical_section("req-1")
    snapshot = await state.snapshot()

    assert deferred == [(2, "req-2")]
    assert snapshot["mutex"]["state"] == RELEASED
    assert snapshot["mutex"]["deferred_replies"] == []


@pytest.mark.asyncio
async def test_timeout_abort_cleans_mutex_state_and_returns_deferred() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})
    await state.begin_mutex_request("req-1")
    await state.handle_mutex_request(mutex_request(2, "req-2", 5, 3), "req-2", 5)

    deferred = await state.abort_mutex_request("req-1", "timeout")
    snapshot = await state.snapshot()

    assert deferred == [(2, "req-2")]
    assert snapshot["mutex"]["state"] == RELEASED
    assert snapshot["mutex"]["request_id"] is None


@pytest.mark.asyncio
async def test_enter_and_exit_critical_section_increment_clock() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})
    await state.begin_mutex_request("req-1")
    await state.handle_mutex_reply(mutex_reply(2, "req-1", 2), "req-1")
    await state.mark_mutex_held("req-1")

    enter = await state.enter_critical_section("req-1")
    exit_event, _ = await state.exit_critical_section("req-1")

    assert enter.logical_time == 4
    assert exit_event.logical_time == 5


def mutex_reply(sender_id: int, request_id: str, logical_time: int) -> AppMessage:
    return AppMessage(
        type=MUTEX_REPLY,
        sender_id=sender_id,
        message_id=f"reply-{sender_id}-{logical_time}",
        logical_time=logical_time,
        payload={"request_id": request_id},
    )


def mutex_request(
    sender_id: int,
    request_id: str,
    request_timestamp: int,
    logical_time: int,
) -> AppMessage:
    return AppMessage(
        type="MUTEX_REQUEST",
        sender_id=sender_id,
        message_id=f"request-{sender_id}-{logical_time}",
        logical_time=logical_time,
        payload={"request_id": request_id, "request_timestamp": request_timestamp},
    )
