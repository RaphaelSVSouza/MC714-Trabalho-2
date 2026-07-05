APP_MESSAGE = "APP_MESSAGE"
MUTEX_REQUEST = "MUTEX_REQUEST"
MUTEX_REPLY = "MUTEX_REPLY"

RELEASED = "RELEASED"
WANTED = "WANTED"
HELD = "HELD"


def has_priority(
    local_timestamp: int,
    local_node_id: int,
    remote_timestamp: int,
    remote_node_id: int,
) -> bool:
    return (local_timestamp, local_node_id) < (remote_timestamp, remote_node_id)


def should_defer_reply(
    mutex_state: str,
    local_request_timestamp: int | None,
    local_node_id: int,
    remote_request_timestamp: int,
    remote_node_id: int,
) -> bool:
    if mutex_state == HELD:
        return True
    if mutex_state == WANTED:
        if local_request_timestamp is None:
            return False
        return has_priority(
            local_request_timestamp,
            local_node_id,
            remote_request_timestamp,
            remote_node_id,
        )
    return False
