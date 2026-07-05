import sys
import time
from uuid import uuid4

import httpx


NODES = {
    1: "http://localhost:8001",
    2: "http://localhost:8002",
    3: "http://localhost:8003",
}


def wait_for_health(client: httpx.Client, node_id: int, base_url: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            response = client.get(f"{base_url}/health")
            if response.status_code == 200 and response.json().get("node_id") == node_id:
                return
            last_error = f"unexpected response from node {node_id}: {response.text}"
        except httpx.HTTPError as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f"node {node_id} did not become healthy: {last_error}")


def get_state(client: httpx.Client, node_id: int) -> dict[str, object]:
    response = client.get(f"{NODES[node_id]}/state")
    response.raise_for_status()
    return response.json()


def get_events(client: httpx.Client, node_id: int) -> list[dict[str, object]]:
    response = client.get(f"{NODES[node_id]}/events")
    response.raise_for_status()
    return response.json()


def wait_for_event(
    client: httpx.Client,
    node_id: int,
    action: str,
    message_id: str | None = None,
    detail: str | None = None,
    timeout: float = 10.0,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for event in get_events(client, node_id):
            if event.get("action") != action:
                continue
            if message_id is not None and event.get("message_id") != message_id:
                continue
            if detail is not None and event.get("detail") != detail:
                continue
            return event
        time.sleep(0.25)
    raise RuntimeError(f"event {action} was not observed on node {node_id}")


def post_json(client: httpx.Client, node_id: int, path: str, payload: dict[str, object]) -> dict[str, object]:
    response = client.post(f"{NODES[node_id]}{path}", json=payload)
    response.raise_for_status()
    return response.json()


def print_timeline(events: list[dict[str, object]]) -> None:
    print("WALL                     L    NODE   ACTION       PEER   MESSAGE")
    for event in events:
        peer = event.get("peer_id") if event.get("peer_id") is not None else "-"
        message = event.get("message_id") or event.get("detail") or "-"
        print(
            f"{event['wall_time']:<23}  {event['logical_time']:<4} "
            f"{event['node_id']:<6} {event['action']:<11} {peer!s:<6} {message}"
        )


def main() -> int:
    try:
        with httpx.Client(timeout=3.0) as client:
            for node_id, base_url in NODES.items():
                wait_for_health(client, node_id, base_url)

            initial_states = {node_id: get_state(client, node_id) for node_id in NODES}
            print(f"initial states: {initial_states}")

            local_description = f"lamport local event {uuid4()}"
            post_json(client, 1, "/commands/local-event", {"description": local_description})
            local_event = wait_for_event(client, 1, "LOCAL", detail=local_description)

            first_send = post_json(
                client,
                1,
                "/commands/send-message",
                {"destination_id": 2, "text": "lamport message node1 to node2"},
            )
            msg_a = first_send["message_id"]
            send_a = wait_for_event(client, 1, "SEND", message_id=msg_a)
            receive_a = wait_for_event(client, 2, "RECEIVE", message_id=msg_a)

            second_send = post_json(
                client,
                2,
                "/commands/send-message",
                {"destination_id": 3, "text": "lamport message node2 to node3"},
            )
            msg_b = second_send["message_id"]
            send_b = wait_for_event(client, 2, "SEND", message_id=msg_b)
            receive_b = wait_for_event(client, 3, "RECEIVE", message_id=msg_b)

            if not send_a["logical_time"] < receive_a["logical_time"]:
                raise RuntimeError("node1->node2 Lamport relation failed")
            if not send_b["logical_time"] < receive_b["logical_time"]:
                raise RuntimeError("node2->node3 Lamport relation failed")
            if not receive_a["logical_time"] < send_b["logical_time"]:
                raise RuntimeError("node2 receive did not happen before node2 send")

            timeline = [local_event, send_a, receive_a, send_b, receive_b]
            print_timeline(timeline)
            print(f"message node1->node2: {msg_a}")
            print(f"message node2->node3: {msg_b}")
            print("SMOKE OK: Lamport timestamps preserve the demonstrated causal order.")
            return 0
    except Exception as exc:
        print(f"SMOKE LAMPORT FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
