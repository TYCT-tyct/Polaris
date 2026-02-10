from __future__ import annotations

import json
from dataclasses import dataclass

import httpx


@dataclass(slots=True)
class GoogleProvider:
    api_key: str | None
    model: str

    @property
    def name(self) -> str:
        return "google"

    def is_configured(self) -> bool:
        return bool(self.api_key)

    async def evaluate(self, prompt: str, timeout_sec: float = 20.0) -> tuple[bool, float, str]:
        if not self.api_key:
            return False, 0.0, "missing_api_key"
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self.model}:generateContent?key={self.api_key}"
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0, "responseMimeType": "application/json"},
            "systemInstruction": {
                "parts": [{"text": "Return strict JSON: {\"allow\":bool,\"confidence\":float,\"reason\":string}"}]
            },
        }
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        parsed = json.loads(text)
        return bool(parsed.get("allow", False)), float(parsed.get("confidence", 0.0)), str(parsed.get("reason", ""))
