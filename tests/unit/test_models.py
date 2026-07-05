import pytest
from pydantic import ValidationError

from node.models import APP_MESSAGE, AppMessage


def test_message_requires_logical_time() -> None:
    with pytest.raises(ValidationError):
        AppMessage(type=APP_MESSAGE, sender_id=1, message_id="msg", payload={})


def test_message_rejects_negative_logical_time() -> None:
    with pytest.raises(ValidationError):
        AppMessage(type=APP_MESSAGE, sender_id=1, message_id="msg", logical_time=-1, payload={})


def test_message_accepts_integer_logical_time() -> None:
    message = AppMessage(type=APP_MESSAGE, sender_id=1, message_id="msg", logical_time=0, payload={})

    assert message.logical_time == 0
