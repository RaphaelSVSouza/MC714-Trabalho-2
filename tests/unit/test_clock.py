from node.clock import LamportClock


def test_initial_value_is_zero() -> None:
    clock = LamportClock()

    assert clock.value == 0


def test_tick_increments_by_one() -> None:
    clock = LamportClock()

    assert clock.tick() == 1
    assert clock.value == 1


def test_successive_ticks_increment_monotonically() -> None:
    clock = LamportClock()

    assert [clock.tick(), clock.tick(), clock.tick()] == [1, 2, 3]


def test_update_with_smaller_received_timestamp() -> None:
    clock = LamportClock()
    clock.value = 5

    assert clock.update(2) == 6


def test_update_with_equal_received_timestamp() -> None:
    clock = LamportClock()
    clock.value = 5

    assert clock.update(5) == 6


def test_update_with_greater_received_timestamp() -> None:
    clock = LamportClock()
    clock.value = 5

    assert clock.update(10) == 11
