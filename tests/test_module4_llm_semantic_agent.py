from __future__ import annotations

import pytest

import polaris.agents.llm_semantic_agent as llm_mod
from polaris.agents.llm_semantic_agent import LLMSemanticAgent, LLMSemanticAgentConfig


def _cfg() -> LLMSemanticAgentConfig:
    return LLMSemanticAgentConfig(
        enabled=True,
        provider="openai",
        model="gpt-5-mini",
        api_key="test-key",
        timeout_sec=1.0,
        max_calls=1,
        confidence_floor=0.55,
        json_strict=True,
        scope="internal_posts_only",
    )


def _posts() -> list[dict]:
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


@pytest.mark.asyncio
async def test_llm_semantic_agent_returns_parsed_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_call(**_: object) -> tuple[str, dict]:
        payload = (
            '{"regime_probs":{"quiet":0.1,"active":0.2,"burst":0.7},'
            '"delta_progress":0.08,"delta_uncertainty":0.12,"mean_shift_hint":0.6,'
            '"confidence":0.83,"evidence_span":["Tonight is gonna be wild"],'
            '"reason_codes":["semantic_intent_burst"]}'
        )
        return payload, {"prompt_tokens": 12, "completion_tokens": 21}

    monkeypatch.setattr(llm_mod, "_call_openai_chat", _fake_call)
    agent = LLMSemanticAgent(_cfg())
    out = await agent.analyze_posts(
        market_id="m1",
        window_code="short",
        as_of=None,
        observed_count=3,
        progress=0.3,
        posts=_posts(),
    )

    assert out.parse_ok is True
    assert out.error_code is None
    assert out.confidence == pytest.approx(0.83)
    assert out.mean_shift_hint == pytest.approx(0.6)
    assert out.regime_probs["burst"] > out.regime_probs["active"]


@pytest.mark.asyncio
async def test_llm_semantic_agent_low_confidence_neutralized(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_call(**_: object) -> tuple[str, dict]:
        return (
            '{"regime_probs":{"quiet":0.2,"active":0.3,"burst":0.5},'
            '"delta_progress":0.09,"delta_uncertainty":0.1,"mean_shift_hint":0.4,'
            '"confidence":0.2,"evidence_span":[],"reason_codes":["weak_signal"]}',
            {},
        )

    monkeypatch.setattr(llm_mod, "_call_openai_chat", _fake_call)
    agent = LLMSemanticAgent(_cfg())
    out = await agent.analyze_posts(
        market_id="m1",
        window_code="short",
        as_of=None,
        observed_count=2,
        progress=0.2,
        posts=_posts(),
    )

    assert out.error_code == "low_confidence"
    assert out.delta_progress == 0.0
    assert out.mean_shift_hint == 0.0
    assert "low_confidence_neutralized" in out.reason_codes


@pytest.mark.asyncio
async def test_llm_semantic_agent_parse_failure_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_call(**_: object) -> tuple[str, dict]:
        return "not-json", {}

    monkeypatch.setattr(llm_mod, "_call_openai_chat", _fake_call)
    agent = LLMSemanticAgent(_cfg())
    out = await agent.analyze_posts(
        market_id="m1",
        window_code="week",
        as_of=None,
        observed_count=7,
        progress=0.7,
        posts=_posts(),
    )

    assert out.parse_ok is False
    assert out.error_code == "json_parse_failed"


@pytest.mark.asyncio
async def test_llm_semantic_agent_parses_json_after_think_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_call(**_: object) -> tuple[str, dict]:
        return (
            "<think>internal reasoning</think>\n"
            '{"regime_probs":{"quiet":0.2,"active":0.3,"burst":0.5},'
            '"delta_progress":0.02,"delta_uncertainty":0.05,"mean_shift_hint":0.2,'
            '"confidence":0.78,"evidence_span":["wild"],"reason_codes":["semantic"]}',
            {},
        )

    monkeypatch.setattr(llm_mod, "_call_openai_chat", _fake_call)
    agent = LLMSemanticAgent(_cfg())
    out = await agent.analyze_posts(
        market_id="m1",
        window_code="short",
        as_of=None,
        observed_count=3,
        progress=0.5,
        posts=_posts(),
    )

    assert out.parse_ok is True
    assert out.error_code is None
    assert out.confidence == pytest.approx(0.78)


@pytest.mark.asyncio
async def test_llm_semantic_agent_minimax_uses_cn_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_call(**kwargs: object) -> tuple[str, dict]:
        captured.update(kwargs)
        return (
            '{"regime_probs":{"quiet":0.2,"active":0.3,"burst":0.5},'
            '"delta_progress":0.01,"delta_uncertainty":0.01,"mean_shift_hint":0.1,'
            '"confidence":0.8,"evidence_span":[],"reason_codes":["ok"]}',
            {},
        )

    monkeypatch.setattr(llm_mod, "_call_openai_chat", _fake_call)
    cfg = _cfg()
    cfg = LLMSemanticAgentConfig(
        enabled=cfg.enabled,
        provider="minimax",
        model="MiniMax-M2.5",
        api_key=cfg.api_key,
        timeout_sec=cfg.timeout_sec,
        max_calls=cfg.max_calls,
        confidence_floor=cfg.confidence_floor,
        json_strict=cfg.json_strict,
        scope=cfg.scope,
    )
    agent = LLMSemanticAgent(cfg)
    out = await agent.analyze_posts(
        market_id="m1",
        window_code="short",
        as_of=None,
        observed_count=4,
        progress=0.4,
        posts=_posts(),
    )

    assert out.error_code is None
    assert captured.get("base_url") == "https://api.minimaxi.com/v1"
