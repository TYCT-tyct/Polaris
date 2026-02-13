from __future__ import annotations

import pytest

from polaris.agents.evidence_agent import EvidenceAgent, EvidenceAgentConfig
from polaris.core.module4.semantic_types import M4SemanticSignal


class _FakeTools:
    async def fetch_recent_posts(self, **_: object) -> list[dict]:
        return [
            {
                "platform_post_id": "p1",
                "posted_at": None,
                "imported_at": None,
                "content": "Tonight is gonna be wild",
                "char_len": 27,
                "is_reply": False,
                "is_retweet": False,
            }
        ]


class _FakeLLM:
    def __init__(self, signal: M4SemanticSignal) -> None:
        self._signal = signal

    async def analyze_posts(self, **_: object) -> M4SemanticSignal:
        return self._signal


@pytest.mark.asyncio
async def test_evidence_agent_neutral_continue_on_error() -> None:
    signal = M4SemanticSignal(
        regime_probs={"quiet": 0.3, "active": 0.2, "burst": 0.5},
        delta_progress=0.1,
        delta_uncertainty=0.15,
        mean_shift_hint=0.5,
        confidence=0.9,
        evidence_span=("Tonight is gonna be wild",),
        reason_codes=("semantic_burst",),
        parse_ok=False,
        llm_model="gpt-5-mini",
        prompt_version="m4-semantic-v1",
        error_code="timeout",
        raw_text="",
        metadata={},
    )
    agent = EvidenceAgent(
        _FakeTools(),
        _FakeLLM(signal),
        EvidenceAgentConfig(
            enabled=True,
            fail_mode="neutral_continue",
            model="gpt-5-mini",
            scope="internal_posts_only",
        ),
    )
    out = await agent.analyze(
        account_id="acc1",
        market_id="m1",
        window_code="short",
        run_tag="rt1",
        observed_count=3,
        progress=0.2,
        as_of=None,
    )
    assert out.degraded is True
    assert out.timed_out is True
    assert out.delta_progress == 0.0
    assert out.mean_shift_hint == 0.0
    assert out.uncertainty_delta == 0.0
    assert "neutral_continue" in out.event_tags


@pytest.mark.asyncio
async def test_evidence_agent_disabled_returns_neutral_evidence() -> None:
    signal = M4SemanticSignal(
        regime_probs={"quiet": 1.0, "active": 0.0, "burst": 0.0},
        delta_progress=0.0,
        delta_uncertainty=0.0,
        mean_shift_hint=0.0,
        confidence=0.0,
    )
    agent = EvidenceAgent(
        _FakeTools(),
        _FakeLLM(signal),
        EvidenceAgentConfig(enabled=False, model="gpt-5-mini"),
    )
    out = await agent.analyze(
        account_id="acc1",
        market_id="m1",
        window_code="short",
        run_tag="rt1",
        observed_count=1,
        progress=0.1,
        as_of=None,
    )
    assert out.error_code == "semantic_disabled"
    assert out.degraded is True
    assert out.calls_used == 0

