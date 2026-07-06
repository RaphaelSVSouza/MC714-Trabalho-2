import subprocess
import sys
import time

import httpx


NODES = {
    1: "http://localhost:8001",
    2: "http://localhost:8002",
    3: "http://localhost:8003",
}
IMPORTANT_ACTIONS = {
    "LEADER_TIMEOUT",
    "ELECTION_STARTED",
    "ELECTION_ROUND_TIMED_OUT",
    "ELECTION_OK_RECEIVED",
    "ELECTION_OK_SENT",
    "COORDINATOR_SENT",
    "COORDINATOR_RECEIVED",
    "COORDINATOR_REJECTED",
    "HEARTBEAT_RECEIVED",
    "HEARTBEAT_REJECTED",
    "LEADER_CHANGED",
    "BECAME_LEADER",
}


def run_compose(*args: str) -> None:
    result = subprocess.run(
        ["docker", "compose", *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"docker compose {' '.join(args)} failed: stdout={result.stdout!r} stderr={result.stderr!r}"
        )


def wait_for_health(client: httpx.Client, node_id: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            response = client.get(f"{NODES[node_id]}/health")
            if response.status_code == 200 and response.json().get("node_id") == node_id:
                return
            last_error = response.text
        except httpx.HTTPError as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f"node{node_id} did not become healthy: {last_error}")


def get_state(client: httpx.Client, node_id: int) -> dict[str, object]:
    response = client.get(f"{NODES[node_id]}/state")
    response.raise_for_status()
    return response.json()


def get_events(client: httpx.Client, node_id: int) -> list[dict[str, object]]:
    response = client.get(f"{NODES[node_id]}/events")
    response.raise_for_status()
    return response.json()


def wait_for_leader(
    client: httpx.Client,
    node_ids: list[int],
    expected_leader: int,
    timeout: float = 25.0,
) -> tuple[dict[int, dict[str, object]], float]:
    start = time.monotonic()
    deadline = start + timeout
    last_states: dict[int, dict[str, object]] = {}
    while time.monotonic() < deadline:
        try:
            last_states = {node_id: get_state(client, node_id) for node_id in node_ids}
            if all(leader_of(state) == expected_leader for state in last_states.values()):
                if all(not election_of(state)["election_in_progress"] for state in last_states.values()):
                    return last_states, (time.monotonic() - start) * 1000
        except httpx.HTTPError:
            pass
        time.sleep(0.25)
    observed = {node_id: election_of(state) for node_id, state in last_states.items()}
    raise RuntimeError(f"leader did not converge to {expected_leader}; observed={observed}")


def election_of(state: dict[str, object]) -> dict[str, object]:
    election = state.get("election")
    if not isinstance(election, dict):
        raise RuntimeError(f"state does not expose election snapshot: {state}")
    return election


def leader_of(state: dict[str, object]) -> object:
    return election_of(state).get("leader_id")


def assert_leader_flags(states: dict[int, dict[str, object]], expected_leader: int) -> None:
    for node_id, state in states.items():
        election = election_of(state)
        expected_flag = node_id == expected_leader
        if election.get("is_leader") is not expected_flag:
            raise RuntimeError(f"node{node_id} is_leader mismatch: {election}")
        if election.get("election_in_progress"):
            raise RuntimeError(f"node{node_id} still has an election in progress: {election}")


def leaders_as_text(states: dict[int, dict[str, object]]) -> str:
    return ",".join(str(leader_of(states[node_id])) for node_id in sorted(states))


def print_phase(phase: str, expected: int, states: dict[int, dict[str, object]], elapsed_ms: float) -> None:
    print(
        f"{phase:<17} {expected:<17} {leaders_as_text(states):<18} ok       {elapsed_ms:.0f}"
    )


def print_recent_events(client: httpx.Client, node_ids: list[int]) -> None:
    print("\nRecent election events:")
    for node_id in node_ids:
        events = [event for event in get_events(client, node_id) if event.get("action") in IMPORTANT_ACTIONS]
        for event in events[-8:]:
            print(
                f"node={node_id} L={event.get('logical_time')} action={event.get('action')} "
                f"peer={event.get('peer_id')} election={event.get('election_id')} "
                f"leader={event.get('leader_id')} type={event.get('message_type')}"
            )


def main() -> int:
    try:
        with httpx.Client(timeout=3.0) as client:
            for node_id in NODES:
                wait_for_health(client, node_id)

            print("PHASE             EXPECTED LEADER   OBSERVED LEADERS   RESULT   TIME_MS")
            initial_states, initial_ms = wait_for_leader(client, [1, 2, 3], 3)
            assert_leader_flags(initial_states, 3)
            print_phase("initial", 3, initial_states, initial_ms)

            lower_message_time = max(state["logical_clock"] for state in initial_states.values()) + 1
            lower_coordinator = client.post(
                f"{NODES[3]}/messages",
                json={
                    "type": "COORDINATOR",
                    "sender_id": 2,
                    "message_id": f"lower-coordinator-{int(time.monotonic() * 1000)}",
                    "logical_time": lower_message_time,
                    "payload": {"leader_id": 2},
                },
            )
            lower_coordinator.raise_for_status()
            if lower_coordinator.json().get("accepted") is not False:
                raise RuntimeError("node3 accepted a lower-priority coordinator")

            lower_heartbeat = client.post(
                f"{NODES[3]}/messages",
                json={
                    "type": "HEARTBEAT",
                    "sender_id": 2,
                    "message_id": f"lower-heartbeat-{int(time.monotonic() * 1000)}",
                    "logical_time": lower_message_time + 1,
                    "payload": {"leader_id": 2},
                },
            )
            lower_heartbeat.raise_for_status()
            if lower_heartbeat.json().get("accepted") is not False:
                raise RuntimeError("node3 accepted a lower-priority heartbeat")

            wait_for_leader(client, [1, 2, 3], 3)
            stopped_at = time.monotonic()
            run_compose("stop", "node3")
            after_stop_states, stopped_ms = wait_for_leader(client, [1, 2], 2)
            assert_leader_flags(after_stop_states, 2)
            print_phase("node3 stopped", 2, after_stop_states, stopped_ms)
            recovery_from_stop_ms = (time.monotonic() - stopped_at) * 1000

            try:
                started_at = time.monotonic()
                run_compose("start", "node3")
                wait_for_health(client, 3)
                recovered_states, recovered_ms = wait_for_leader(client, [1, 2, 3], 3)
                assert_leader_flags(recovered_states, 3)
                print_phase("node3 recovered", 3, recovered_states, recovered_ms)
                recovery_to_node3_ms = (time.monotonic() - started_at) * 1000
            finally:
                run_compose("start", "node3")

            print_recent_events(client, [1, 2, 3])
            print(f"time to elect node2 after stop: {recovery_from_stop_ms:.0f} ms")
            print(f"time to restore node3 leadership: {recovery_to_node3_ms:.0f} ms")
            print("SMOKE OK: Bully election converged after initial startup, leader stop, and recovery.")
            return 0
    except Exception as exc:
        print(f"SMOKE ELECTION FAILED: {exc}", file=sys.stderr)
        try:
            run_compose("start", "node3")
        except Exception as restore_exc:
            print(f"failed to restore node3: {restore_exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
