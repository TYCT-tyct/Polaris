from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

from polaris.core.module4.types import M4Evidence

from .evidence_tools import EvidenceTools
from .llm_semantic_agent import LLMSemanticAgent


@dataclass(frozen=True)
class EvidenceAgentConfig:
    enabled: bool = True
    max_calls: int = 1
    timeout_sec: float = 3.0
    confidence_floor: float = 0.55
    fail_mode: str = "neutral_continue"
    model: str = "gpt-5-mini"
    provider: str = "openai"
    api_key: str | None = None
    json_strict: bool = True
    scope: str = "internal_posts_only"


class EvidenceAgent:
    """LLM-only semantic analyzer. No keyword or hardcoded semantic rules."""

    def __init__(self, tools: EvidenceTools, llm_agent: LLMSemanticAgent, config: EvidenceAgentConfig | None = None) -> None:
        self._tools = tools
        self._llm = llm_agent
        self._cfg = config or EvidenceAgentConfig()

    async def analyze(
        self,
        *,
        account_id: str,
        market_id: str,
        window_code: str,
        run_tag: str,
        observed_count: int,
        progress: float,
        as_of: datetime | None = None,
    ) -> M4Evidence:
        if not self._cfg.enabled:
            return _neutral_evidence(
                reason="semantic_disabled",
                calls_used=0,
                timed_out=False,
                degraded=True,
                model=self._cfg.model,
            )

        started = time.perf_counter()
        posts = await self._tools.fetch_recent_posts(
            account_id=account_id,
            lookback_minutes=240 if window_code == "week" else 120,
            as_of=as_of,
            limit=120,
        )

        signal = await self._llm.analyze_posts(
            market_id=market_id,
            window_code=window_code,
            as_of=as_of,
            observed_count=observed_count,
            progress=progress,
            posts=posts,
        )
        latency_ms = (time.perf_counter() - started) * 1000.0

        timed_out = signal.error_code == "timeout"
        degraded = bool(signal.error_code)
        delta_progress = signal.delta_progress
        delta_uncertainty = signal.delta_uncertainty
        mean_shift_hint = signal.mean_shift_hint
        event_tags = tuple(sorted(set((*signal.reason_codes, *signal.regime_probs.keys()))))
        if signal.error_code and self._cfg.fail_mode == "neutral_continue":
            delta_progress = 0.0
            delta_uncertainty = 0.0
            mean_shift_hint = 0.0
            event_tags = tuple(sorted(set((*event_tags, "neutral_continue"))))

        sources = tuple(
            {
                "source": "fact_tweet_post",
                "count": len(posts),
                "scope": self._cfg.scope,
            }
            for _ in range(1)
        )
        return M4Evidence(
            event_tags=event_tags,
            confidence=signal.confidence,
            delta_progress=delta_progress,
            uncertainty_delta=delta_uncertainty,
            mean_shift_hint=mean_shift_hint,
            calls_used=1,
            timed_out=timed_out,
            degraded=degraded,
            parse_ok=signal.parse_ok,
            llm_model=signal.llm_model,
            prompt_version=signal.prompt_version,
            error_code=signal.error_code,
            latency_ms=latency_ms,
            sources=sources,
            metadata={
                "run_tag": run_tag,
                "scope": self._cfg.scope,
                "fail_mode": self._cfg.fail_mode,
                "evidence_span": list(signal.evidence_span),
                "reason_codes": list(signal.reason_codes),
                "raw_text": signal.raw_text[:6000],
                "signal_meta": signal.metadata,
            },
        )


def _neutral_evidence(
    *,
    reason: str,
    calls_used: int,
    timed_out: bool,
    degraded: bool,
    model: str,
) -> M4Evidence:
    return M4Evidence(
        event_tags=(reason,),
        confidence=0.0,
        delta_progress=0.0,
        uncertainty_delta=0.0,
        mean_shift_hint=0.0,
        calls_used=calls_used,
        timed_out=timed_out,
        degraded=degraded,
        parse_ok=False,
        llm_model=model,
        prompt_version="",
        error_code=reason,
        latency_ms=0.0,
        sources=(),
        metadata={"reason": reason},
    )

