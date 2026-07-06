import time

import pytest

from node.election import ElectionConfig, higher_peers, lower_peers, should_accept_coordinator, should_accept_election_ok
from node.models import COORDINATOR, ELECTION, ELECTION_OK, HEARTBEAT, AppMessage
from node.mutex import RELEASED
from node.state import NodeState


def test_higher_and_lower_peers_are_selected_by_node_id() -> None:
    peers = {1: "http://node1:8000", 3: "http://node3:8000", 4: "http://node4:8000"}

    assert higher_peers(2, peers) == [(3, "http://node3:8000"), (4, "http://node4:8000")]
    assert lower_peers(2, peers) == [(1, "http://node1:8000")]
    assert higher_peers(4, peers) == []


@pytest.mark.parametrize(
    "node_id,current_leader_id,announced_leader_id,expected",
    [
        (1, None, 1, True),
        (1, None, 2, True),
        (1, None, 3, True),
        (2, None, 1, False),
        (3, None, 2, False),
        (1, 2, 3, True),
        (1, 3, 2, False),
        (2, 3, 2, False),
        (3, 3, 3, True),
    ],
)
def test_coordinator_acceptance_matrix(
    node_id: int,
    current_leader_id: int | None,
    announced_leader_id: int,
    expected: bool,
) -> None:
    assert should_accept_coordinator(node_id, current_leader_id, announced_leader_id) is expected


def test_election_config_requires_coherent_positive_timeouts() -> None:
    ElectionConfig(700, 2500, 700, 2500, 500).validate()

    with pytest.raises(ValueError):
        ElectionConfig(700, 700, 700, 2500, 500).validate()
    with pytest.raises(ValueError):
        ElectionConfig(0, 2500, 700, 2500, 500).validate()


def test_election_ok_acceptance_rule() -> None:
    assert should_accept_election_ok(1, 2, "e1", "e1", True) is True
    assert should_accept_election_ok(1, 2, "e1", "old", True) is False
    assert should_accept_election_ok(2, 1, "e1", "e1", True) is False
    assert should_accept_election_ok(1, 2, "e1", "e1", False) is False


@pytest.mark.asyncio
async def test_begin_election_creates_id_clears_old_replies_and_lists_higher_peers() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})
    state.election_ok_received = {2}

    started, higher = await state.begin_election("e1", time.monotonic(), "test")
    snapshot = await state.snapshot()

    assert started is True
    assert higher == [(2, "http://node2:8000"), (3, "http://node3:8000")]
    assert snapshot["election"]["election_in_progress"] is True
    assert snapshot["election"]["election_id"] == "e1"
    assert snapshot["election"]["ok_received_from"] == []
    assert snapshot["election"]["elections_started"] == 1


@pytest.mark.asyncio
async def test_begin_election_does_not_duplicate_active_round() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})
    await state.begin_election("e1", time.monotonic(), "first")

    started, higher = await state.begin_election("e2", time.monotonic(), "second")

    assert started is False
    assert higher == []
    assert (await state.snapshot())["election"]["election_id"] == "e1"


@pytest.mark.asyncio
async def test_highest_node_becomes_leader_without_higher_peers() -> None:
    state = NodeState(node_id=3, peers={1: "http://node1:8000", 2: "http://node2:8000"})
    started, higher = await state.begin_election("e1", time.monotonic(), "startup")

    became, peers, event = await state.become_leader("e1", time.monotonic())
    snapshot = await state.snapshot()

    assert started is True
    assert higher == []
    assert became is True
    assert peers == [(1, "http://node1:8000"), (2, "http://node2:8000")]
    assert event is not None
    assert snapshot["election"]["leader_id"] == 3
    assert snapshot["election"]["is_leader"] is True
    assert snapshot["election"]["election_in_progress"] is False


@pytest.mark.asyncio
async def test_node_with_higher_id_answers_election() -> None:
    state = NodeState(node_id=2, peers={1: "http://node1:8000", 3: "http://node3:8000"})

    should_reply = await state.handle_election(message(ELECTION, 1, 4, {"election_id": "e1"}), "e1")

    assert should_reply is True


@pytest.mark.asyncio
async def test_election_ok_valid_duplicate_old_and_lower_sender() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})
    await state.begin_election("e1", time.monotonic(), "test")

    assert await state.handle_election_ok(message(ELECTION_OK, 2, 2, {"election_id": "e1"}), "e1") is True
    assert await state.handle_election_ok(message(ELECTION_OK, 2, 3, {"election_id": "e1"}), "e1") is False
    assert await state.handle_election_ok(message(ELECTION_OK, 3, 4, {"election_id": "old"}), "old") is False
    assert await state.handle_election_ok(message(ELECTION_OK, 0, 5, {"election_id": "e1"}), "e1") is False

    snapshot = await state.snapshot()
    assert snapshot["election"]["ok_received_from"] == [2]


@pytest.mark.asyncio
async def test_coordinator_updates_leader_and_clears_election() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})
    await state.begin_election("e1", time.monotonic(), "test")
    await state.handle_election_ok(message(ELECTION_OK, 2, 2, {"election_id": "e1"}), "e1")

    accepted = await state.handle_coordinator(message(COORDINATOR, 3, 3, {"leader_id": 3}), 3)
    snapshot = await state.snapshot()

    assert accepted is True
    assert snapshot["election"]["leader_id"] == 3
    assert snapshot["election"]["election_in_progress"] is False
    assert snapshot["election"]["ok_received_from"] == []


@pytest.mark.asyncio
async def test_lower_coordinator_is_rejected_during_active_election() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})
    await state.begin_election("e1", time.monotonic(), "test")
    await state.handle_heartbeat(message(HEARTBEAT, 3, 2, {"leader_id": 3}), 3)

    accepted = await state.handle_coordinator(message(COORDINATOR, 2, 4, {"leader_id": 2}), 2)
    snapshot = await state.snapshot()

    assert accepted is False
    assert snapshot["election"]["leader_id"] == 3
    assert snapshot["election"]["election_in_progress"] is True
    assert snapshot["election"]["election_id"] == "e1"


@pytest.mark.asyncio
async def test_rejected_coordinator_does_not_clear_active_election() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})
    state.leader_id = 3
    await state.begin_election("e1", time.monotonic(), "test")

    accepted = await state.handle_coordinator(message(COORDINATOR, 2, 4, {"leader_id": 2}), 2)
    snapshot = await state.snapshot()

    assert accepted is False
    assert snapshot["election"]["election_in_progress"] is True
    assert snapshot["election"]["election_id"] == "e1"
    assert snapshot["election"]["leader_id"] == 3


@pytest.mark.asyncio
async def test_node_rejects_coordinator_lower_than_its_own_id() -> None:
    state = NodeState(node_id=3, peers={1: "http://node1:8000", 2: "http://node2:8000"})

    accepted = await state.handle_coordinator(message(COORDINATOR, 2, 2, {"leader_id": 2}), 2)
    snapshot = await state.snapshot()

    assert accepted is False
    assert snapshot["election"]["leader_id"] is None
    assert snapshot["election"]["election_in_progress"] is False


@pytest.mark.asyncio
async def test_higher_coordinator_is_accepted_during_active_election() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})
    await state.begin_election("e1", time.monotonic(), "test")

    accepted = await state.handle_coordinator(message(COORDINATOR, 3, 3, {"leader_id": 3}), 3)
    snapshot = await state.snapshot()

    assert accepted is True
    assert snapshot["election"]["leader_id"] == 3
    assert snapshot["election"]["election_in_progress"] is False
    assert snapshot["election"]["election_id"] is None


@pytest.mark.asyncio
async def test_lower_heartbeat_does_not_replace_higher_leader() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})
    await state.handle_heartbeat(message(HEARTBEAT, 3, 2, {"leader_id": 3}), 3)

    accepted = await state.handle_heartbeat(message(HEARTBEAT, 2, 3, {"leader_id": 2}), 2)
    snapshot = await state.snapshot()

    assert accepted is False
    assert snapshot["election"]["leader_id"] == 3


@pytest.mark.asyncio
async def test_node_rejects_heartbeat_lower_than_its_own_id() -> None:
    state = NodeState(node_id=3, peers={1: "http://node1:8000", 2: "http://node2:8000"})

    accepted = await state.handle_heartbeat(message(HEARTBEAT, 2, 2, {"leader_id": 2}), 2)
    snapshot = await state.snapshot()

    assert accepted is False
    assert snapshot["election"]["leader_id"] is None


@pytest.mark.asyncio
async def test_valid_heartbeat_updates_leader_time_and_invalid_does_not_replace_higher_leader() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000", 3: "http://node3:8000"})

    assert await state.handle_heartbeat(message(HEARTBEAT, 3, 2, {"leader_id": 3}), 3) is True
    assert await state.handle_heartbeat(message(HEARTBEAT, 2, 3, {"leader_id": 2}), 2) is False

    snapshot = await state.snapshot()
    assert snapshot["election"]["leader_id"] == 3


@pytest.mark.asyncio
async def test_leader_timeout_clears_leader_and_leader_does_not_timeout_itself() -> None:
    state = NodeState(node_id=1, peers={2: "http://node2:8000"})
    await state.handle_heartbeat(message(HEARTBEAT, 2, 2, {"leader_id": 2}), 2)

    assert await state.record_leader_timeout(time.monotonic() + 3, timeout_seconds=2) is True
    assert (await state.snapshot())["election"]["leader_id"] is None

    leader = NodeState(node_id=2, peers={1: "http://node1:8000"})
    await leader.begin_election("e2", time.monotonic(), "test")
    await leader.become_leader("e2", time.monotonic())
    assert await leader.record_leader_timeout(time.monotonic() + 3, timeout_seconds=2) is False


@pytest.mark.asyncio
async def test_election_messages_do_not_change_mutex_state() -> None:
    state = NodeState(node_id=2, peers={1: "http://node1:8000", 3: "http://node3:8000"})

    await state.handle_election(message(ELECTION, 1, 2, {"election_id": "e1"}), "e1")
    await state.handle_heartbeat(message(HEARTBEAT, 3, 3, {"leader_id": 3}), 3)

    assert (await state.snapshot())["mutex"]["state"] == RELEASED


def message(message_type: str, sender_id: int, logical_time: int, payload: dict[str, object]) -> AppMessage:
    return AppMessage(
        type=message_type,
        sender_id=sender_id,
        message_id=f"{message_type}-{sender_id}-{logical_time}",
        logical_time=logical_time,
        payload=payload,
    )
