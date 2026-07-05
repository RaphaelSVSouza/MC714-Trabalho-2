class LamportClock:
    def __init__(self) -> None:
        self.value = 0

    def tick(self) -> int:
        self.value += 1
        return self.value

    def update(self, received_time: int) -> int:
        self.value = max(self.value, received_time) + 1
        return self.value
