from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from polaris.core.module4.semantic_types import M4SemanticSignal

from .llm_prompt_templates import PROMPT_VERSION, SYSTEM_PROMPT, build_semantic_user_prompt


@dataclass(frozen=True)
class LLMSemanticAgentConfig:
    enabled: bool = True
    provider: str = "openai"
    model: str = "gpt-5-mini"
    api_key: str | None = None
    base_url: str | None = None
    timeout_sec: float = 3.0
    max_calls: int = 1
    confidence_floor: float = 0.55
    json_strict: bool = True
    scope: str = "internal_posts_only"


class LLMSemanticAgent:
    def __init__(self, config: LLMSemanticAgentConfig) -> None:
        self._cfg = config

    async def analyze_posts(
        self,
        *,
        market_id: str,
        window_code: str,
        as_of: datetime | None,
        observed_count: int,
        progress: float,
        posts: list[dict[str, Any]],
    ) -> M4SemanticSignal:
        provider = (self._cfg.provider or "").strip().lower()
        if not self._cfg.enabled:
            return self._disabled("semantic_disabled")
        if not self._cfg.api_key:
            return self._disabled("missing_api_key")
        if provider not in {"openai", "minimax"}:
            return self._disabled("unsupported_provider")
        if not posts:
            return self._disabled("no_recent_posts")

        prompt = build_semantic_user_prompt(
            market_id=market_id,
            window_code=window_code,
            as_of=as_of,
            observed_count=observed_count,
            progress=progress,
            posts=posts,
        )

        try:
            async with asyncio.timeout(max(0.2, self._cfg.timeout_sec)):
                content, usage = await _call_openai_chat(
                    api_key=self._cfg.api_key,
                    model=self._cfg.model,
                    system_prompt=SYSTEM_PROMPT,
                    user_prompt=prompt,
                    timeout_sec=self._cfg.timeout_sec,
                    base_url=self._cfg.base_url or _default_base_url(provider),
                )
        except TimeoutError:
            return self._error("timeout", raw_text="")
        except Exception as exc:  # noqa: BLE001
            return self._error(type(exc).__name__, raw_text="")

        try:
            parsed = _parse_json_payload(content, strict=self._cfg.json_strict)
        except Exception as exc:  # noqa: BLE001
            return self._error("json_parse_failed", raw_text=content, detail=str(exc))

        signal = _to_signal(parsed, model=self._cfg.model, usage=usage, raw_text=content)
        if signal.confidence < self._cfg.confidence_floor:
            return M4SemanticSignal(
                regime_probs=signal.regime_probs,
                delta_progress=0.0,
                delta_uncertainty=max(0.0, signal.delta_uncertainty),
                mean_shift_hint=0.0,
                confidence=signal.confidence,
                evidence_span=signal.evidence_span,
                reason_codes=tuple(sorted(set((*signal.reason_codes, "low_confidence_neutralized")))),
                parse_ok=signal.parse_ok,
                llm_model=signal.llm_model,
                prompt_version=signal.prompt_version,
                error_code="low_confidence",
                raw_text=signal.raw_text,
                metadata=signal.metadata,
            )
        return signal

    def _disabled(self, code: str) -> M4SemanticSignal:
        return M4SemanticSignal(
            regime_probs={"quiet": 1.0, "active": 0.0, "burst": 0.0},
            delta_progress=0.0,
            delta_uncertainty=0.0,
            mean_shift_hint=0.0,
            confidence=0.0,
            evidence_span=(),
            reason_codes=(code,),
            parse_ok=True,
            llm_model=self._cfg.model,
            prompt_version=PROMPT_VERSION,
            error_code=code,
            raw_text="",
            metadata={"disabled": True},
        )

    def _error(self, code: str, *, raw_text: str, detail: str | None = None) -> M4SemanticSignal:
        md: dict[str, Any] = {"disabled": False}
        if detail:
            md["error_detail"] = detail
        return M4SemanticSignal(
            regime_probs={"quiet": 1.0, "active": 0.0, "burst": 0.0},
            delta_progress=0.0,
            delta_uncertainty=0.0,
            mean_shift_hint=0.0,
            confidence=0.0,
            evidence_span=(),
            reason_codes=(code,),
            parse_ok=False,
            llm_model=self._cfg.model,
            prompt_version=PROMPT_VERSION,
            error_code=code,
            raw_text=raw_text,
            metadata=md,
        )


async def _call_openai_chat(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    timeout_sec: float,
    base_url: str = "https://api.openai.com/v1",
) -> tuple[str, dict[str, Any]]:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        resp = await client.post(f"{base_url.rstrip('/')}/chat/completions", headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    content = str(data["choices"][0]["message"]["content"])
    usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    return content, usage


def _parse_json_payload(text: str, *, strict: bool) -> dict[str, Any]:
    raw = text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        candidate = _extract_first_json_object(raw)
        if candidate is None:
            raise
        parsed = json.loads(candidate)
    if strict and not isinstance(parsed, dict):
        raise ValueError("semantic payload must be a JSON object")
    if isinstance(parsed, dict):
        return parsed
    return {}


def _extract_first_json_object(text: str) -> str | None:
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escaped = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _to_signal(payload: dict[str, Any], *, model: str, usage: dict[str, Any], raw_text: str) -> M4SemanticSignal:
    probs = payload.get("regime_probs") if isinstance(payload.get("regime_probs"), dict) else {}
    regime_probs = _normalize_regime_probs(
        {
            "quiet": float(probs.get("quiet") or 0.0),
            "active": float(probs.get("active") or 0.0),
            "burst": float(probs.get("burst") or 0.0),
        }
    )
    delta_progress = _clip(float(payload.get("delta_progress") or 0.0), -0.15, 0.15)
    delta_uncertainty = _clip(float(payload.get("delta_uncertainty") or 0.0), 0.0, 0.25)
    mean_shift_hint = _clip(float(payload.get("mean_shift_hint") or 0.0), -1.0, 1.0)
    confidence = _clip(float(payload.get("confidence") or 0.0), 0.0, 1.0)
    evidence_span = tuple(str(x) for x in (payload.get("evidence_span") or []) if str(x).strip())[:6]
    reason_codes = tuple(str(x) for x in (payload.get("reason_codes") or []) if str(x).strip())[:8]
    return M4SemanticSignal(
        regime_probs=regime_probs,
        delta_progress=delta_progress,
        delta_uncertainty=delta_uncertainty,
        mean_shift_hint=mean_shift_hint,
        confidence=confidence,
        evidence_span=evidence_span,
        reason_codes=reason_codes,
        parse_ok=True,
        llm_model=model,
        prompt_version=PROMPT_VERSION,
        error_code=None,
        raw_text=raw_text,
        metadata={"usage": usage, "created_at": datetime.now(tz=UTC).isoformat()},
    )


def _normalize_regime_probs(values: dict[str, float]) -> dict[str, float]:
    total = sum(max(0.0, v) for v in values.values())
    if total <= 0:
        return {"quiet": 1.0, "active": 0.0, "burst": 0.0}
    return {k: max(0.0, v) / total for k, v in values.items()}


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _default_base_url(provider: str) -> str:
    if provider == "minimax":
        # CN endpoint by default for domestic routing.
        return "https://api.minimaxi.com/v1"
    return "https://api.openai.com/v1"
