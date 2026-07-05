from __future__ import annotations

import asyncio
import csv
import json
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx


ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "docs" / "results"
NODES = {
    1: "http://localhost:8001",
    2: "http://localhost:8002",
    3: "http://localhost:8003",
}
RESOURCE = "http://localhost:8010"
PROTOCOL_TYPES = {"MUTEX_REQUEST", "MUTEX_REPLY"}
TIMEOUTS = {
    "health_timeout_s": 30.0,
    "leader_timeout_s": 30.0,
    "resource_timeout_s": 15.0,
    "mutex_timeout_s": 20.0,
    "election_stop_timeout_s": 30.0,
}
HEARTBEAT_CONFIG = {
    "HEARTBEAT_INTERVAL_MS": 700,
    "LEADER_TIMEOUT_MS": 2500,
    "ELECTION_RESPONSE_TIMEOUT_MS": 700,
    "COORDINATOR_TIMEOUT_MS": 2500,
    "STARTUP_ELECTION_DELAY_MS": 500,
}


def run_compose(*args: str) -> None:
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"docker compose {' '.join(args)} failed with code {result.returncode}: "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )


def wait_for_health(client: httpx.Client, url: str, expected_node_id: int | None = None, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            response = client.get(f"{url}/health")
            if response.status_code == 200:
                payload = response.json()
                if expected_node_id is None or payload.get("node_id") == expected_node_id:
                    return
                last_error = f"unexpected health payload {payload!r}"
            else:
                last_error = response.text
        except httpx.HTTPError as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f"{url} did not become healthy: {last_error}")


def get_json(client: httpx.Client, url: str) -> object:
    response = client.get(url)
    response.raise_for_status()
    return response.json()


def post_json(client: httpx.Client, url: str, payload: dict[str, object] | None = None) -> object:
    response = client.post(url, json=payload or {})
    response.raise_for_status()
    return response.json()


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
    *,
    message_id: str | None = None,
    detail: str | None = None,
    request_id: str | None = None,
    timeout: float = 15.0,
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
            if request_id is not None and event.get("request_id") != request_id:
                continue
            return event
        time.sleep(0.2)
    raise RuntimeError(f"event {action} was not observed on node {node_id}")


def wait_for_resource_counts(entries: int, exits: int, timeout: float = 15.0) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=3.0) as client:
        while time.monotonic() < deadline:
            state = get_json(client, f"{RESOURCE}/state")
            if state["entries"] == entries and state["exits"] == exits:
                return state
            time.sleep(0.2)
    raise RuntimeError(f"resource did not reach entries={entries} exits={exits}")


def wait_for_leader(
    client: httpx.Client,
    node_ids: list[int],
    expected_leader: int,
    timeout: float = 30.0,
) -> tuple[dict[int, dict[str, object]], float]:
    start = time.monotonic()
    deadline = start + timeout
    last_states: dict[int, dict[str, object]] = {}
    while time.monotonic() < deadline:
        try:
            last_states = {node_id: get_state(client, node_id) for node_id in node_ids}
            if all(state["election"]["leader_id"] == expected_leader for state in last_states.values()):
                if all(not state["election"]["election_in_progress"] for state in last_states.values()):
                    return last_states, (time.monotonic() - start) * 1000
        except httpx.HTTPError:
            pass
        time.sleep(0.25)
    observed = {node_id: state.get("election") for node_id, state in last_states.items()}
    raise RuntimeError(f"leader did not converge to {expected_leader}; observed={observed}")


def isolate_stack(build: bool = False) -> None:
    run_compose("down")
    if build:
        run_compose("up", "--build", "-d")
    else:
        run_compose("up", "-d")


def start_fresh_stack() -> None:
    run_compose("down")
    run_compose("up", "--build", "-d")
    with httpx.Client(timeout=3.0) as client:
        for node_id, base_url in NODES.items():
            wait_for_health(client, base_url, expected_node_id=node_id, timeout=TIMEOUTS["health_timeout_s"])
        wait_for_health(client, RESOURCE, timeout=TIMEOUTS["health_timeout_s"])
        wait_for_leader(client, [1, 2, 3], 3, timeout=TIMEOUTS["leader_timeout_s"])


def reset_resource() -> None:
    with httpx.Client(timeout=3.0) as client:
        post_json(client, f"{RESOURCE}/reset")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def duration_ms(start_wall: str, end_wall: str) -> float:
    return (parse_iso(end_wall) - parse_iso(start_wall)).total_seconds() * 1000


def protocol_message_count(client: httpx.Client, request_id: str) -> int:
    total = 0
    for node_id in NODES:
        for event in get_events(client, node_id):
            if event.get("request_id") != request_id:
                continue
            if event.get("action") == "SEND" and event.get("message_type") in PROTOCOL_TYPES:
                total += 1
    return total


def deferred_reply_count(client: httpx.Client, request_id: str) -> int:
    total = 0
    for node_id in NODES:
        for event in get_events(client, node_id):
            if event.get("action") == "MUTEX_REPLY_DEFERRED" and event.get("request_id") == request_id:
                total += 1
    return total


def collect_lamport_repetition(rep: int) -> dict[str, object]:
    isolate_stack(build=False)
    with httpx.Client(timeout=3.0) as client:
        for node_id, base_url in NODES.items():
            wait_for_health(client, base_url, expected_node_id=node_id)
        wait_for_health(client, RESOURCE)
        wait_for_leader(client, [1, 2, 3], 3)

        local_description = f"experiment-lamport-{rep}-{int(time.time() * 1000)}"
        post_json(client, f"{NODES[1]}/commands/local-event", {"description": local_description})
        local_event = wait_for_event(client, 1, "LOCAL", detail=local_description)

        send_12 = post_json(
            client,
            f"{NODES[1]}/commands/send-message",
            {"destination_id": 2, "text": f"lamport rep {rep} 1->2"},
        )
        msg_12 = str(send_12["message_id"])
        send_event_12 = wait_for_event(client, 1, "SEND", message_id=msg_12)
        recv_event_12 = wait_for_event(client, 2, "RECEIVE", message_id=msg_12)

        send_23 = post_json(
            client,
            f"{NODES[2]}/commands/send-message",
            {"destination_id": 3, "text": f"lamport rep {rep} 2->3"},
        )
        msg_23 = str(send_23["message_id"])
        send_event_23 = wait_for_event(client, 2, "SEND", message_id=msg_23)
        recv_event_23 = wait_for_event(client, 3, "RECEIVE", message_id=msg_23)

        relations = {
            "LOCAL(node1) < SEND(node1)": local_event["logical_time"] < send_event_12["logical_time"],
            "SEND(node1) < RECEIVE(node2)": send_event_12["logical_time"] < recv_event_12["logical_time"],
            "RECEIVE(node2) < SEND(node2)": recv_event_12["logical_time"] < send_event_23["logical_time"],
            "SEND(node2) < RECEIVE(node3)": send_event_23["logical_time"] < recv_event_23["logical_time"],
        }
        violations = sum(1 for ok in relations.values() if not ok)
        return {
            "rep": rep,
            "local_event": local_event,
            "messages": [
                {
                    "direction": "node1->node2",
                    "message_id": msg_12,
                    "send": send_event_12,
                    "receive": recv_event_12,
                },
                {
                    "direction": "node2->node3",
                    "message_id": msg_23,
                    "send": send_event_23,
                    "receive": recv_event_23,
                },
            ],
            "relations": relations,
            "violations": violations,
        }


def collect_mutex_single_repetition(rep: int) -> dict[str, object]:
    reset_resource()
    with httpx.Client(timeout=15.0) as client:
        initial_states = {node_id: get_state(client, node_id) for node_id in NODES}
        response = post_json(
            client,
            f"{NODES[1]}/commands/request-critical-section",
            {"duration_ms": 150},
        )
        request_id = str(response["request_id"])
        resource_state = wait_for_resource_counts(entries=1, exits=1)
        resource_events = get_json(client, f"{RESOURCE}/events")
        node_events = {node_id: get_events(client, node_id) for node_id in NODES}
        final_states = {node_id: get_state(client, node_id) for node_id in NODES}
        protocol_messages = protocol_message_count(client, request_id)
        if not resource_events:
            raise RuntimeError("resource events missing after mutex request")
        enter_event = next(event for event in resource_events if event["action"] == "ENTER")
        exit_event = next(event for event in resource_events if event["action"] == "EXIT")
        return {
            "rep": rep,
            "request_id": request_id,
            "request_timestamp": response["request_timestamp"],
            "wait_ms": response["wait_ms"],
            "resource_duration_ms": duration_ms(enter_event["wall_time"], exit_event["wall_time"]),
            "protocol_messages": protocol_messages,
            "violations": resource_state["violations"],
            "initial_states": initial_states,
            "final_states": final_states,
            "resource_events": resource_events,
            "node_events": node_events,
        }


async def request_cs(client: httpx.AsyncClient, node_id: int, duration_ms: int) -> dict[str, object]:
    response = await client.post(
        f"{NODES[node_id]}/commands/request-critical-section",
        json={"duration_ms": duration_ms},
        timeout=20.0,
    )
    response.raise_for_status()
    return response.json()


def collect_mutex_concurrent_repetition(rep: int) -> dict[str, object]:
    reset_resource()

    async def run() -> list[dict[str, object]]:
        async with httpx.AsyncClient(timeout=20.0) as client:
            return await asyncio.gather(
                request_cs(client, 1, 200),
                request_cs(client, 2, 200),
                request_cs(client, 3, 200),
            )

    results = asyncio.run(run())
    with httpx.Client(timeout=15.0) as client:
        resource_state = wait_for_resource_counts(entries=3, exits=3)
        resource_events = get_json(client, f"{RESOURCE}/events")
        final_states = {node_id: get_state(client, node_id) for node_id in NODES}
        if len(resource_events) < 6:
            raise RuntimeError("resource events missing for concurrent mutex experiment")
        order: list[dict[str, object]] = []
        for index, event in enumerate(resource_events, start=1):
            if event["action"] == "ENTER":
                order.append(
                    {
                        "order": index,
                        "node_id": event["node_id"],
                        "request_id": event["request_id"],
                        "enter": event["wall_time"],
                        "overlap": bool(event["overlap"]),
                    }
                )
        detailed_results: list[dict[str, object]] = []
        for result in results:
            request_id = str(result["request_id"])
            request_node = next(node_id for node_id, state in final_states.items() if any(
                event.get("request_id") == request_id and event.get("action") == "MUTEX_WANTED"
                for event in get_events(client, node_id)
            ))
            enter_event = next(event for event in resource_events if event["request_id"] == request_id and event["action"] == "ENTER")
            exit_event = next(event for event in resource_events if event["request_id"] == request_id and event["action"] == "EXIT")
            detailed_results.append(
                {
                    "node_id": request_node,
                    "request_id": request_id,
                    "request_timestamp": result["request_timestamp"],
                    "status": result["status"],
                    "wait_ms": result["wait_ms"],
                    "resource_duration_ms": duration_ms(enter_event["wall_time"], exit_event["wall_time"]),
                    "deferred_replies": deferred_reply_count(client, request_id),
                    "protocol_messages": protocol_message_count(client, request_id),
                }
            )
        return {
            "rep": rep,
            "requests": detailed_results,
            "resource_order": order,
            "violations": resource_state["violations"],
            "final_states": final_states,
        }


def collect_election_repetition(rep: int) -> dict[str, object]:
    start_fresh_stack()
    with httpx.Client(timeout=3.0) as client:
        initial_states, initial_ms = wait_for_leader(client, [1, 2, 3], 3)
        initial_summary = {
            "leaders": {str(node_id): state["election"]["leader_id"] for node_id, state in initial_states.items()},
            "is_leader": {str(node_id): state["election"]["is_leader"] for node_id, state in initial_states.items()},
            "convergence_ms": initial_ms,
            "elections_started": {str(node_id): state["election"]["elections_started"] for node_id, state in initial_states.items()},
        }

        stop_started = time.monotonic()
        run_compose("stop", "node3")
        after_stop_states, stop_ms = wait_for_leader(client, [1, 2], 2)
        stop_summary = {
            "leaders": {str(node_id): state["election"]["leader_id"] for node_id, state in after_stop_states.items()},
            "is_leader": {str(node_id): state["election"]["is_leader"] for node_id, state in after_stop_states.items()},
            "convergence_ms": stop_ms,
            "elapsed_ms": (time.monotonic() - stop_started) * 1000,
            "elections_started": {str(node_id): state["election"]["elections_started"] for node_id, state in after_stop_states.items()},
        }

        start_started = time.monotonic()
        run_compose("start", "node3")
        wait_for_health(client, NODES[3], expected_node_id=3)
        recovered_states, recovery_ms = wait_for_leader(client, [1, 2, 3], 3)
        recovery_summary = {
            "leaders": {str(node_id): state["election"]["leader_id"] for node_id, state in recovered_states.items()},
            "is_leader": {str(node_id): state["election"]["is_leader"] for node_id, state in recovered_states.items()},
            "convergence_ms": recovery_ms,
            "elapsed_ms": (time.monotonic() - start_started) * 1000,
            "elections_started": {str(node_id): state["election"]["elections_started"] for node_id, state in recovered_states.items()},
        }

        return {
            "rep": rep,
            "initial": initial_summary,
            "after_stop": stop_summary,
            "after_recovery": recovery_summary,
            "divergence_after_stabilization": len({state["election"]["leader_id"] for state in recovered_states.values()}) != 1,
        }


def summarize_numeric(values: list[float | int]) -> dict[str, float | int]:
    return {
        "min": min(values),
        "max": max(values),
        "mean": statistics.mean(values),
        "median": statistics.median(values),
    }


def build_summary_rows(results: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    lamport_violations = [rep["violations"] for rep in results["lamport"]]
    rows.append(
        {
            "experiment": "lamport_causality",
            "metric": "violations",
            "repetitions": len(lamport_violations),
            **summarize_numeric(lamport_violations),
            "violations": sum(lamport_violations),
            "observations": "zero violations expected",
        }
    )

    mutex_single_messages = [rep["protocol_messages"] for rep in results["mutex_single"]]
    mutex_single_wait = [rep["wait_ms"] for rep in results["mutex_single"]]
    mutex_single_duration = [rep["resource_duration_ms"] for rep in results["mutex_single"]]
    mutex_single_violations = [rep["violations"] for rep in results["mutex_single"]]
    rows.extend(
        [
            {
                "experiment": "mutex_single",
                "metric": "protocol_messages",
                "repetitions": len(mutex_single_messages),
                **summarize_numeric(mutex_single_messages),
                "violations": sum(mutex_single_violations),
                "observations": "expected 4 messages per request for 3 nodes",
            },
            {
                "experiment": "mutex_single",
                "metric": "wait_ms",
                "repetitions": len(mutex_single_wait),
                **summarize_numeric(mutex_single_wait),
                "violations": sum(mutex_single_violations),
                "observations": "waiting time for single CS request",
            },
            {
                "experiment": "mutex_single",
                "metric": "resource_duration_ms",
                "repetitions": len(mutex_single_duration),
                **summarize_numeric(mutex_single_duration),
                "violations": sum(mutex_single_violations),
                "observations": "observed duration in resource log",
            },
        ]
    )

    mutex_concurrent_violations = [rep["violations"] for rep in results["mutex_concurrent"]]
    mutex_concurrent_waits = [request["wait_ms"] for rep in results["mutex_concurrent"] for request in rep["requests"]]
    mutex_concurrent_durations = [request["resource_duration_ms"] for rep in results["mutex_concurrent"] for request in rep["requests"]]
    mutex_concurrent_deferred = [request["deferred_replies"] for rep in results["mutex_concurrent"] for request in rep["requests"]]
    rows.extend(
        [
            {
                "experiment": "mutex_concurrent",
                "metric": "wait_ms",
                "repetitions": len(mutex_concurrent_waits),
                **summarize_numeric(mutex_concurrent_waits),
                "violations": sum(mutex_concurrent_violations),
                "observations": "three concurrent requests",
            },
            {
                "experiment": "mutex_concurrent",
                "metric": "resource_duration_ms",
                "repetitions": len(mutex_concurrent_durations),
                **summarize_numeric(mutex_concurrent_durations),
                "violations": sum(mutex_concurrent_violations),
                "observations": "observed critical-section duration",
            },
            {
                "experiment": "mutex_concurrent",
                "metric": "deferred_replies",
                "repetitions": len(mutex_concurrent_deferred),
                **summarize_numeric(mutex_concurrent_deferred),
                "violations": sum(mutex_concurrent_violations),
                "observations": "responses deferred under RA",
            },
        ]
    )

    election_detection = [rep["after_stop"]["convergence_ms"] for rep in results["election"]]
    election_recovery = [rep["after_recovery"]["convergence_ms"] for rep in results["election"]]
    election_total = [rep["initial"]["convergence_ms"] + rep["after_stop"]["convergence_ms"] + rep["after_recovery"]["convergence_ms"] for rep in results["election"]]
    election_divergences = [1 if rep["divergence_after_stabilization"] else 0 for rep in results["election"]]
    rows.extend(
        [
            {
                "experiment": "election",
                "metric": "stop_detection_ms",
                "repetitions": len(election_detection),
                **summarize_numeric(election_detection),
                "violations": sum(election_divergences),
                "observations": "node3 stopped then node2 became leader",
            },
            {
                "experiment": "election",
                "metric": "recovery_ms",
                "repetitions": len(election_recovery),
                **summarize_numeric(election_recovery),
                "violations": sum(election_divergences),
                "observations": "node3 recovered and reassumed leadership",
            },
            {
                "experiment": "election",
                "metric": "total_phase_ms",
                "repetitions": len(election_total),
                **summarize_numeric(election_total),
                "violations": sum(election_divergences),
                "observations": "initial convergence + failure + recovery",
            },
        ]
    )

    return rows


def write_json(path: Path, data: dict[str, object]) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=True), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise RuntimeError("no summary rows generated")
    fieldnames = ["experiment", "metric", "repetitions", "min", "max", "mean", "median", "violations", "observations"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> int:
    started = datetime.now(timezone.utc).isoformat()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = RESULTS_DIR / "raw-results.json"
    csv_path = RESULTS_DIR / "experiment-summary.csv"
    readme_path = RESULTS_DIR / "README.md"

    results: dict[str, list[dict[str, object]]] = {
        "lamport": [],
        "mutex_single": [],
        "mutex_concurrent": [],
        "election": [],
    }

    try:
        for rep in range(1, 6):
            results["lamport"].append(collect_lamport_repetition(rep))

        start_fresh_stack()
        for rep in range(1, 6):
            results["mutex_single"].append(collect_mutex_single_repetition(rep))
            results["mutex_concurrent"].append(collect_mutex_concurrent_repetition(rep))

        for rep in range(1, 6):
            results["election"].append(collect_election_repetition(rep))
            run_compose("down")

        summary_rows = build_summary_rows(results)
        raw_data = {
            "generated_at_utc": started,
            "environment": {
                "python": sys.version.split()[0],
                "node_count": 3,
                "timeouts": TIMEOUTS,
                "election_config_ms": HEARTBEAT_CONFIG,
            },
            "experiments": results,
        }
        write_json(raw_path, raw_data)
        write_csv(csv_path, summary_rows)
        readme_path.write_text(
            "\n".join(
                [
                    "# Results",
                    "",
                    f"Generated at (UTC): {started}",
                    "",
                    "Environment:",
                    "- Three nodes plus the `resource` observer",
                    "- Python 3.12",
                    "- Docker Compose local stack",
                    "- Timeout configuration matches `docker-compose.yml`",
                    "",
                    "Commands used:",
                    "- `python scripts/run_experiments.py`",
                    "- `docker compose up --build -d`",
                    "- `docker compose stop node3` / `docker compose start node3` for election recovery",
                    "",
                    "Metric notes:",
                    "- `violations`: count of observed causality or safety violations in the repetition",
                    "- `min` / `max` / `mean` / `median`: computed from the numeric metric column in `experiment-summary.csv`",
                    "- `observations`: short human-readable note about what the metric represents",
                    "",
                    "The JSON and CSV files are generated from the live stack and should be treated as the source of truth for the report.",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        print(f"Wrote {raw_path}")
        print(f"Wrote {csv_path}")
        print(f"Wrote {readme_path}")
        return 0
    except Exception as exc:
        print(f"EXPERIMENTS FAILED: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            run_compose("down")
        except Exception as cleanup_exc:
            print(f"cleanup failed: {cleanup_exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())


