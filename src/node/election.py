from dataclasses import dataclass


@dataclass(frozen=True)
class ElectionConfig:
    heartbeat_interval_ms: int
    leader_timeout_ms: int
    election_response_timeout_ms: int
    coordinator_timeout_ms: int
    startup_election_delay_ms: int

    def validate(self) -> None:
        values = {
            "HEARTBEAT_INTERVAL_MS": self.heartbeat_interval_ms,
            "LEADER_TIMEOUT_MS": self.leader_timeout_ms,
            "ELECTION_RESPONSE_TIMEOUT_MS": self.election_response_timeout_ms,
            "COORDINATOR_TIMEOUT_MS": self.coordinator_timeout_ms,
            "STARTUP_ELECTION_DELAY_MS": self.startup_election_delay_ms,
        }
        for name, value in values.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.leader_timeout_ms <= self.heartbeat_interval_ms:
            raise ValueError("LEADER_TIMEOUT_MS must be greater than HEARTBEAT_INTERVAL_MS")

    @property
    def heartbeat_interval_seconds(self) -> float:
        return self.heartbeat_interval_ms / 1000

    @property
    def leader_timeout_seconds(self) -> float:
        return self.leader_timeout_ms / 1000

    @property
    def election_response_timeout_seconds(self) -> float:
        return self.election_response_timeout_ms / 1000

    @property
    def coordinator_timeout_seconds(self) -> float:
        return self.coordinator_timeout_ms / 1000

    @property
    def startup_election_delay_seconds(self) -> float:
        return self.startup_election_delay_ms / 1000


def higher_peers(node_id: int, peers: dict[int, str]) -> list[tuple[int, str]]:
    return [(peer_id, peers[peer_id]) for peer_id in sorted(peers) if peer_id > node_id]

def lower_peers(node_id: int, peers: dict[int, str]) -> list[tuple[int, str]]:
    return [(peer_id, peers[peer_id]) for peer_id in sorted(peers) if peer_id < node_id]



def should_accept_election_ok(
    node_id: int,
    sender_id: int,
    local_election_id: str | None,
    received_election_id: str,
    election_in_progress: bool,
) -> bool:
    return (
        election_in_progress
        and local_election_id == received_election_id
        and sender_id > node_id
    )


def should_accept_coordinator(current_leader_id: int | None, announced_leader_id: int) -> bool:
    return current_leader_id is None or announced_leader_id >= current_leader_id


