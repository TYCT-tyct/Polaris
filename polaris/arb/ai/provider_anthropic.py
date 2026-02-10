from __future__ import annotations

import json
from dataclasses import dataclass

import httpx


@dataclass(slots=True)
class AnthropicProvider:
    api_key: str | None
    model: str

    @property
    def name(self) -> str:
        return "anthropic"

    def is_configured(self) -> bool:
        return bool(self.api_key)

    async def evaluate(self, prompt: str, timeout_sec: float = 20.0) -> tuple[bool, float, str]:
        if not self.api_key:
            return False, 0.0, "missing_api_key"
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": self.model,
            "max_tokens": 256,
            "temperature": 0,
            "system": "Return strict JSON: {\"allow\":bool,\"confidence\":float,\"reason\":string}",
            "messages": [{"role": "user", "content": prompt}],
        }
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.post("https://api.anthropic.com/v1/messages", headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")
        parsed = _parse_json(text)
        return bool(parsed.get("allow", False)), float(parsed.get("confidence", 0.0)), str(parsed.get("reason", ""))


def _parse_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text)
