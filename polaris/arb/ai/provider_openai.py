from __future__ import annotations

import json
from dataclasses import dataclass

import httpx


@dataclass(slots=True)
class OpenAIProvider:
    api_key: str | None
    model: str

    @property
    def name(self) -> str:
        return "openai"

    def is_configured(self) -> bool:
        return bool(self.api_key)

    async def evaluate(self, prompt: str, timeout_sec: float = 20.0) -> tuple[bool, float, str]:
        if not self.api_key:
            return False, 0.0, "missing_api_key"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "Return strict JSON: {\"allow\":bool,\"confidence\":float,\"reason\":string}"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0,
        }
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
        content = data["choices"][0]["message"]["content"]
        parsed = _parse_json(content)
        return bool(parsed.get("allow", False)), float(parsed.get("confidence", 0.0)), str(parsed.get("reason", ""))


def _parse_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text)
