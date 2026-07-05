import asyncio
import sys
import time

import httpx


NODES = {
    1: "http://localhost:8001",
    2: "http://localhost:8002",
    3: "http://localhost:8003",
}
RESOURCE = "http://localhost:8010"
PROTOCOL_TYPES = {"MUTEX_REQUEST", "MUTEX_REPLY"}


def wait_for_health(client: httpx.Client, name: str, base_url: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            response = client.get(f"{base_url}/health")
            if response.status_code == 200:
                return
            last_error = response.text
        except httpx.HTTPError as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f"{name} did not become healthy: {last_error}")


def get_json(client: httpx.Client, url: str) -> object:
    response = client.get(url)
    response.raise_for_status()
    return response.json()


def post_json(client: httpx.Client, url: str, payload: dict[str, object] | None = None) -> object:
    response = client.post(url, json=payload or {})
    response.raise_for_status()
    return response.json()


def get_node_state(client: httpx.Client, node_id: int) -> dict[str, object]:
    return get_json(client, f"{NODES[node_id]}/state")


def get_node_events(client: httpx.Client, node_id: int) -> list[dict[str, object]]:
    return get_json(client, f"{NODES[node_id]}/events")


def get_resource_state(client: httpx.Client) -> dict[str, object]:
    return get_json(client, f"{RESOURCE}/state")


def get_resource_events(client: httpx.Client) -> list[dict[str, object]]:
    return get_json(client, f"{RESOURCE}/events")


def wait_for_resource_counts(entries: int, exits: int, timeout: float = 10.0) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=3.0) as client:
        while time.monotonic() < deadline:
            state = get_resource_state(client)
            if state["entries"] == entries and state["exits"] == exits:
                return state
            time.sleep(0.25)
    raise RuntimeError(f"resource did not reach entries={entries} exits={exits}")


def assert_all_nodes_released(client: httpx.Client) -> None:
    for node_id in NODES:
        state = get_node_state(client, node_id)
        mutex = state["mutex"]
        if mutex["state"] != "RELEASED":
            raise RuntimeError(f"node {node_id} is not RELEASED: {mutex}")
        if mutex["deferred_replies"]:
            raise RuntimeError(f"node {node_id} still has deferred replies: {mutex}")


def count_protocol_sends(client: httpx.Client, request_id: str) -> int:
    total = 0
    failures = []
    for node_id in NODES:
        for event in get_node_events(client, node_id):
            if event.get("request_id") != request_id:
                continue
            if event.get("action") == "SEND" and event.get("message_type") in PROTOCOL_TYPES:
                total += 1
            if event.get("action") == "SEND_FAILED" and event.get("message_type") in PROTOCOL_TYPES:
                failures.append(event)
    if failures:
        raise RuntimeError(f"protocol send failures found for {request_id}: {failures}")
    return total


def print_resource_order(events: list[dict[str, object]]) -> None:
    by_request: dict[str, dict[str, object]] = {}
    order: list[str] = []
    for event in events:
        request_id = str(event["request_id"])
        if request_id not in by_request:
            by_request[request_id] = {"request_id": request_id, "overlap": False}
            order.append(request_id)
        row = by_request[request_id]
        row["node_id"] = event["node_id"]
        if event["action"] == "ENTER":
            row["enter"] = event["wall_time"]
            row["overlap"] = bool(event["overlap"])
        elif event["action"] == "EXIT":
            row["exit"] = event["wall_time"]

    print("ORDER  NODE  REQUEST                               ENTER                    EXIT                     OVERLAP")
    for index, request_id in enumerate(order, start=1):
        row = by_request[request_id]
        print(
            f"{index:<6} {row.get('node_id', '-')!s:<5} {request_id:<37} "
            f"{row.get('enter', '-'):<24} {row.get('exit', '-'):<24} "
            f"{'yes' if row.get('overlap') else 'no'}"
        )


def phase_single_request() -> str:
    with httpx.Client(timeout=15.0) as client:
        post_json(client, f"{RESOURCE}/reset")
        initial = {node_id: get_node_state(client, node_id) for node_id in NODES}
        print(f"phase A initial states: {initial}")
        result = post_json(
            client,
            f"{NODES[1]}/commands/request-critical-section",
            {"duration_ms": 150},
        )
        request_id = result["request_id"]
        resource_state = wait_for_resource_counts(entries=1, exits=1)
        if resource_state["violations"] != 0:
            raise RuntimeError(f"resource reported violations: {resource_state}")
        assert_all_nodes_released(client)
        protocol_messages = count_protocol_sends(client, request_id)
        if protocol_messages != 4:
            raise RuntimeError(f"expected 4 protocol messages, got {protocol_messages}")
        print(f"phase A result: {result}")
        print(f"phase A protocol messages for {request_id}: {protocol_messages}")
        print_resource_order(get_resource_events(client))
        return request_id


async def request_cs_async(client: httpx.AsyncClient, node_id: int, duration_ms: int) -> dict[str, object]:
    response = await client.post(
        f"{NODES[node_id]}/commands/request-critical-section",
        json={"duration_ms": duration_ms},
        timeout=20.0,
    )
    response.raise_for_status()
    return response.json()


def phase_concurrent_requests() -> list[str]:
    with httpx.Client(timeout=3.0) as client:
        post_json(client, f"{RESOURCE}/reset")

    async def run() -> list[dict[str, object]]:
        async with httpx.AsyncClient(timeout=20.0) as client:
            return await asyncio.gather(
                request_cs_async(client, 1, 200),
                request_cs_async(client, 2, 200),
                request_cs_async(client, 3, 200),
            )

    results = asyncio.run(run())
    request_ids = [str(result["request_id"]) for result in results]

    resource_state = wait_for_resource_counts(entries=3, exits=3, timeout=15.0)
    if resource_state["violations"] != 0:
        raise RuntimeError(f"resource reported violations: {resource_state}")

    with httpx.Client(timeout=3.0) as client:
        assert_all_nodes_released(client)
        events = get_resource_events(client)
        print(f"phase B results: {results}")
        print_resource_order(events)
    return request_ids


def main() -> int:
    try:
        with httpx.Client(timeout=3.0) as client:
            for node_id, base_url in NODES.items():
                wait_for_health(client, f"node{node_id}", base_url)
            wait_for_health(client, "resource", RESOURCE)

        single_request = phase_single_request()
        concurrent_requests = phase_concurrent_requests()
        print(f"phase A request: {single_request}")
        print(f"phase B requests: {concurrent_requests}")
        print("SMOKE OK: Ricart-Agrawala completed without observed overlap.")
        return 0
    except Exception as exc:
        print(f"SMOKE MUTEX FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
