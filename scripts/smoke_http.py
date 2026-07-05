import sys
import time

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


def wait_for_received_message(
    client: httpx.Client,
    base_url: str,
    message_id: str,
    timeout: float = 10.0,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(f"{base_url}/events")
        response.raise_for_status()
        events = response.json()
        for event in events:
            if event.get("action") == "RECEIVE" and event.get("message_id") == message_id:
                return event
        time.sleep(0.25)
    raise RuntimeError(f"message {message_id} was not observed on destination node")


def main() -> int:
    try:
        with httpx.Client(timeout=3.0) as client:
            for node_id, base_url in NODES.items():
                wait_for_health(client, node_id, base_url)

            node1_before = get_state(client, 1)
            node2_before = get_state(client, 2)
            print(f"node1 initial state: {node1_before}")
            print(f"node2 initial state: {node2_before}")

            command = {"destination_id": 2, "text": "smoke test from node1 to node2"}
            response = client.post(f"{NODES[1]}/commands/send-message", json=command)
            response.raise_for_status()
            result = response.json()
            message_id = result["message_id"]

            event = wait_for_received_message(client, NODES[2], message_id)
            node1_after = get_state(client, 1)
            node2_after = get_state(client, 2)

            if node1_after["messages_sent"] < node1_before["messages_sent"] + 1:
                raise RuntimeError("node1 messages_sent did not increment")
            if node2_after["messages_received"] < node2_before["messages_received"] + 1:
                raise RuntimeError("node2 messages_received did not increment")

            print(f"sent message id: {message_id}")
            print(f"node2 receive event: {event}")
            print(f"node1 final state: {node1_after}")
            print(f"node2 final state: {node2_after}")
            print("SMOKE OK: node1 sent an HTTP/JSON message and node2 received it.")
            return 0
    except Exception as exc:
        print(f"SMOKE FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

