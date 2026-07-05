import httpx

from .models import AppMessage


async def send_message(peer_url: str, message: AppMessage, timeout: float = 2.0) -> dict[str, object]:
    url = f"{peer_url.rstrip('/')}/messages"
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(url, json=message.as_dict())
        response.raise_for_status()
        return response.json()


async def post_json(url: str, payload: dict[str, object], timeout: float = 2.0) -> dict[str, object]:
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
        return response.json()
