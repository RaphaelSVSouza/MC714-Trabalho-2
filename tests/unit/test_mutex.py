from node.mutex import HELD, RELEASED, WANTED, has_priority, should_defer_reply


def test_priority_with_smaller_local_timestamp() -> None:
    assert has_priority(1, 2, 2, 1) is True


def test_priority_with_smaller_remote_timestamp() -> None:
    assert has_priority(3, 1, 2, 2) is False


def test_priority_tie_with_smaller_local_node_id() -> None:
    assert has_priority(5, 1, 5, 2) is True


def test_priority_tie_with_smaller_remote_node_id() -> None:
    assert has_priority(5, 3, 5, 2) is False


def test_released_replies_immediately() -> None:
    assert should_defer_reply(RELEASED, None, 1, 2, 2) is False


def test_held_defers_reply() -> None:
    assert should_defer_reply(HELD, None, 1, 2, 2) is True


def test_wanted_with_local_priority_defers_reply() -> None:
    assert should_defer_reply(WANTED, 1, 1, 2, 2) is True


def test_wanted_without_local_priority_replies_immediately() -> None:
    assert should_defer_reply(WANTED, 5, 1, 2, 2) is False
